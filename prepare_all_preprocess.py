import csv
import os
import requests
import zipfile
import io
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

# --- CONFIGURATION ---
T212_FILE = 'trading212.csv'
REVOLUT_FILE = 'revolut.csv'
IBKR_FILE = 'ibkr.csv'

# Outputs
MASTER_FILE = 'master_data.csv'
AUDIT_FILE = 'audit_rates.csv'  # Proof of exchange rates
SKIPPED_FILE = 'audit_skipped.csv'  # Report of ignored rows OR Missing ISINs
ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"


# ---------------------

class CurrencyConverter:
    def __init__(self):
        self.rates = {}
        self.load_ecb_rates()

    def load_ecb_rates(self):
        print(f"Downloading official exchange rates from ECB...")
        try:
            r = requests.get(ECB_URL)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            with z.open('eurofxref-hist.csv') as f:
                df = pd.read_csv(f)
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
            self.rates = df.to_dict(orient='index')
        except Exception as e:
            print(f"Error loading ECB rates: {e}")

    def get_rate(self, date_str, currency):
        if currency == 'EUR': return 1.0, date_str, 1.0
        target_date = pd.to_datetime(date_str)
        for i in range(5):
            check_date = target_date - timedelta(days=i)
            if check_date in self.rates:
                rate_row = self.rates[check_date]
                if currency in rate_row and not pd.isna(rate_row[currency]):
                    raw_rate = rate_row[currency]
                    return 1 / raw_rate, check_date.strftime('%Y-%m-%d'), raw_rate
        return None, None, None


def find_isin_online(ticker):
    """Attempts to find ISIN using yfinance."""
    print(f"  ... Searching online for ISIN: {ticker}")
    try:
        t = yf.Ticker(ticker)
        # Check if isin exists and is not a placeholder
        if hasattr(t, 'isin') and t.isin and t.isin != '-' and len(t.isin) > 0:
            return t.isin
    except:
        pass
    return ''


def clean_number(value):
    if not value: return 0.0
    if isinstance(value, (float, int)): return float(value)
    clean = str(value).replace('$', '').replace('€', '').replace('£', '').replace(',', '')
    try:
        return float(clean)
    except:
        return 0.0


# --- PROCESSORS ---

def process_revolut(file_path, converter, audit_log, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path} (Revolut)...")
    df = pd.read_csv(file_path)

    for idx, row in df.iterrows():
        r_type = str(row.get('Type', '')).upper()
        ticker = row.get('Ticker', '') or row.get('Symbol', '')
        desc = str(row.get('Description', '')).upper()

        # Determine Type
        trans_type = ''
        if r_type in ['BUY', 'MARKET BUY']:
            trans_type = 'BUY'
        elif r_type in ['SELL', 'MARKET SELL']:
            trans_type = 'SELL'
        elif r_type in ['DIVIDEND', 'DIV']:
            trans_type = 'DIV'
        elif r_type == 'INTEREST' or 'SAVINGS' in desc:
            trans_type = 'INTEREST'

        if not trans_type:
            skipped_log.append(
                {'Source': 'Revolut', 'Row': idx, 'Reason': f'Type {r_type} not relevant', 'RawData': str(row.values)})
            continue

        if trans_type in ['BUY', 'SELL', 'DIV'] and (not ticker or pd.isna(ticker)):
            skipped_log.append(
                {'Source': 'Revolut', 'Row': idx, 'Reason': 'Stock trade missing Ticker', 'RawData': str(row.values)})
            continue

        # Date & Currency
        raw_date = row.get('Date', '') or row.get('Completed Date', '')
        try:
            date_str = pd.to_datetime(raw_date).strftime('%Y-%m-%d')
        except:
            skipped_log.append({'Source': 'Revolut', 'Row': idx, 'Reason': 'Invalid Date', 'RawData': raw_date})
            continue

        amount = abs(clean_number(row.get('Total Amount', 0) or row.get('Amount', 0)))
        currency = row.get('Currency', 'USD')

        final_eur = amount
        if currency != 'EUR':
            rate, found_date, raw_ecb = converter.get_rate(date_str, currency)
            if rate:
                final_eur = amount * rate
                audit_log.append(
                    {'Date': date_str, 'Source': 'Revolut', 'Ticker': ticker, 'OrigAmount': amount, 'Curr': currency,
                     'RateUsed': raw_ecb, 'FinalEUR': final_eur})
            else:
                print(f"  [WARNING] No ECB rate for {currency} on {date_str}")

        rows.append({
            'Source': 'Revolut', 'Date': date_str, 'Type': trans_type,
            'Ticker': ticker if ticker else 'CASH',
            'Quantity': clean_number(row.get('Quantity', 0)),
            'TotalValueEUR': final_eur,
            'ISIN': '',
            'Name': ticker if ticker else 'Revolut Interest',
            'TaxPaidEUR': 0
        })
    return rows


def process_t212(file_path, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path} (Trading212)...")
    df = pd.read_csv(file_path)

    for idx, row in df.iterrows():
        action = str(row.get('Action', '')).lower()

        trans_type = ''
        if 'buy' in action:
            trans_type = 'BUY'
        elif 'sell' in action:
            trans_type = 'SELL'
        elif 'dividend' in action:
            trans_type = 'DIV'
        elif 'lending' in action:
            trans_type = 'LENDING'
        elif 'interest' in action:
            trans_type = 'INTEREST'
        else:
            skipped_log.append(
                {'Source': 'T212', 'Row': idx, 'Reason': f'Action {action} skipped', 'RawData': str(row.values)})
            continue

        date_str = pd.to_datetime(row.get('Time')).strftime('%Y-%m-%d')
        total = clean_number(row.get('Total (EUR)', 0))
        if total == 0: total = clean_number(row.get('Total', 0)) * clean_number(row.get('Exchange rate', 1))

        rows.append({
            'Source': 'T212', 'Date': date_str, 'Type': trans_type,
            'Ticker': row.get('Ticker', '') or 'CASH',
            'Quantity': clean_number(row.get('No. of shares', 0)),
            'TotalValueEUR': abs(total),
            'ISIN': row.get('ISIN', ''),
            'Name': row.get('Name', '') or 'T212 Interest',
            'TaxPaidEUR': abs(clean_number(row.get('Withholding tax (EUR)', 0)))
        })
    return rows


def process_ibkr(file_path, converter, audit_log, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path} (IBKR)...")
    try:
        df = pd.read_csv(file_path, skiprows=lambda x: x > 0 and 'Trades' not in str(x))
        # Add IBKR logic here if needed
    except:
        pass
    return rows


def main():
    converter = CurrencyConverter()
    audit_log = []
    skipped_log = []
    all_rows = []

    # 1. Process Files
    all_rows.extend(process_revolut(REVOLUT_FILE, converter, audit_log, skipped_log))
    all_rows.extend(process_t212(T212_FILE, skipped_log))
    all_rows.extend(process_ibkr(IBKR_FILE, converter, audit_log, skipped_log))

    # 2. ISIN Lookup (Only for Stocks)
    print("Verifying ISINs for Stocks...")
    # Map existing ISINs from files first
    existing_isins = {r['Ticker']: r['ISIN'] for r in all_rows if r['ISIN'] and r['Type'] in ['BUY', 'SELL']}

    # Track which tickers we already searched online to avoid repeated web calls
    searched_tickers = set()

    for r in all_rows:
        if r['Type'] in ['BUY', 'SELL'] and not r['ISIN']:
            ticker = r['Ticker']

            if ticker in existing_isins:
                # Use cached/known ISIN
                r['ISIN'] = existing_isins[ticker]

            else:
                # Not in cache, try web search (only once per ticker)
                if ticker not in searched_tickers:
                    searched_tickers.add(ticker)
                    found = find_isin_online(ticker)

                    if found:
                        existing_isins[ticker] = found
                        r['ISIN'] = found
                    else:
                        # --- NEW LOGIC: Log missing ISIN ---
                        msg = f"ISIN NULL/Not Found for Ticker: {ticker}"
                        print(f"  [WARNING] {msg}")
                        skipped_log.append({
                            'Source': 'ISIN_CHECK',
                            'Row': 'N/A',
                            'Reason': msg,
                            'RawData': f"Ticker: {ticker}"
                        })
                else:
                    # If we searched before and found nothing, it remains empty
                    if ticker in existing_isins:
                        r['ISIN'] = existing_isins[ticker]

    # 3. Save Files
    all_rows.sort(key=lambda x: x['Date'])

    keys = ['Source', 'Date', 'Type', 'Ticker', 'ISIN', 'Name', 'Quantity', 'TotalValueEUR', 'TaxPaidEUR']
    pd.DataFrame(all_rows)[keys].to_csv(MASTER_FILE, index=False)

    if audit_log: pd.DataFrame(audit_log).to_csv(AUDIT_FILE, index=False)
    if skipped_log: pd.DataFrame(skipped_log).to_csv(SKIPPED_FILE, index=False)

    print("\n" + "=" * 30)
    print(f"DONE! Files Generated:")
    print(f"1. {MASTER_FILE} (Data for XML)")
    print(f"2. {AUDIT_FILE} (Currency Rates Used)")
    print(f"3. {SKIPPED_FILE} (Errors & Missing ISINs - CHECK THIS)")
    print("=" * 30)


# ... (Imports and Config remain the same) ...

# ---------------------------------------------------------
# NEW STRICT CLEANER
# ---------------------------------------------------------
def clean_isin(value):
    """
    Forces invalid values (NaN, float('nan'), None, 'nan') to be an empty string.
    This prevents 'NaN' from being treated as a valid existing ISIN.
    """
    if value is None:
        return ''

    # Check for actual Pandas/Numpy NaN objects
    if pd.isna(value):
        return ''

    s = str(value).strip()

    # Check for string representations of nothing
    if s.lower() in ['nan', 'none', '', '-']:
        return ''

    return s


# ... (CurrencyConverter and clean_number remain the same) ...

def find_isin_online(ticker):
    print(f"  [WEB SEARCH] Looking up ISIN for: {ticker}...")
    try:
        t = yf.Ticker(ticker)
        # yfinance sometimes returns '-' for empty ISINs, handle that
        isin = t.isin
        if isin and isin != '-' and len(isin) > 0:
            print(f"    -> FOUND: {isin}")
            return isin
    except Exception as e:
        print(f"    -> ERROR looking up {ticker}: {e}")

    print(f"    -> FAILED (No data found)")
    return ''


# ... (Keep process_revolut, process_t212, process_ibkr SAME as before) ...
# ... BUT ensure you use `clean_isin()` inside them like you did in the previous step ...
# ... If you need me to paste those whole functions again, let me know.
# ... The CRITICAL fix is in the main() logic below:

def main():
    converter = CurrencyConverter()
    audit_log = []
    skipped_log = []
    all_rows = []

    # 1. Process Files
    # We apply clean_isin IMMEDIATELY inside these functions, or we fix it here after loading
    print("Loading files...")
    all_rows.extend(process_revolut(REVOLUT_FILE, converter, audit_log, skipped_log))
    all_rows.extend(process_t212(T212_FILE, skipped_log))
    all_rows.extend(process_ibkr(IBKR_FILE, converter, audit_log, skipped_log))

    # --- SAFETY FIX: SANITIZE ALL ROWS FIRST ---
    # This ensures no NaN objects survive to trick the logic
    for r in all_rows:
        r['ISIN'] = clean_isin(r.get('ISIN'))
    # -------------------------------------------

    # 2. ISIN Lookup
    print("\nVerifying ISINs...")

    # Build cache ONLY from rows that have a REAL string for ISIN
    existing_isins = {}
    for r in all_rows:
        if r['ISIN']:  # Since we sanitized above, this is safe now (empty string is False)
            existing_isins[r['Ticker']] = r['ISIN']

    print(f"DEBUG: {len(existing_isins)} tickers already have valid ISINs in your files.")
    print(existing_isins)

    searched_tickers = set()

    for r in all_rows:
        # Only care about BUY/SELL
        if r['Type'] not in ['BUY', 'SELL']:
            continue

        ticker = r['Ticker']

        # If we already have a valid ISIN in this specific row, skip
        if r['ISIN']:
            continue

        # If we know this ISIN from a different row/file, copy it
        if ticker in existing_isins:
            r['ISIN'] = existing_isins[ticker]
            continue

        # --- WEB SEARCH ---
        # If we haven't searched this ticker yet this session
        if ticker not in searched_tickers:
            searched_tickers.add(ticker)

            # Go online
            found_isin = find_isin_online(ticker)

            if found_isin:
                # Update current row
                r['ISIN'] = found_isin
                # Add to cache so next rows for same stock get it instantly
                existing_isins[ticker] = found_isin
            else:
                # LOG THE FAILURE
                msg = f"ISIN NOT FOUND for {ticker}"
                print(f"  [AUDIT WARNING] {msg}")
                skipped_log.append({
                    'Source': 'ISIN_CHECK',
                    'Row': 'N/A',
                    'Reason': msg,
                    'RawData': f"Ticker: {ticker}"
                })
        else:
            # We already searched this ticker and found nothing
            # Just ensure we didn't miss a cache update
            if ticker in existing_isins:
                r['ISIN'] = existing_isins[ticker]

    # 3. Save
    all_rows.sort(key=lambda x: x['Date'])
    keys = ['Source', 'Date', 'Type', 'Ticker', 'ISIN', 'Name', 'Quantity', 'TotalValueEUR', 'TaxPaidEUR']

    pd.DataFrame(all_rows)[keys].to_csv(MASTER_FILE, index=False)
    if audit_log: pd.DataFrame(audit_log).to_csv(AUDIT_FILE, index=False)
    if skipped_log: pd.DataFrame(skipped_log).to_csv(SKIPPED_FILE, index=False)

    print("\n" + "=" * 30)
    print("DONE.")
    print(f"Check {SKIPPED_FILE} for any tickers that failed web lookup.")
    print("=" * 30)


if __name__ == "__main__":
    main()