import csv
import os
import requests
import zipfile
import io
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# --- CONFIGURATION ---
T212_FILE = 'trading212.csv'
REVOLUT_FILE = 'revolut.csv'
IBKR_FILE = 'ibkr.csv'

MASTER_FILE = 'master_data.csv'
AUDIT_FILE = 'audit_rates.csv'
SKIPPED_FILE = 'audit_skipped.csv'
ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"

# --- MANUAL OVERRIDE ---
MANUAL_ISIN_MAP = {
    'HPQ': 'US40434L1052',
    'O': 'US7561091049',
}


# ---------------------

class CurrencyConverter:
    def __init__(self):
        self.rates = {}
        self.load_ecb_rates()

    def load_ecb_rates(self):
        print(f"Downloading official exchange rates from ECB...")
        try:
            r = requests.get(ECB_URL, timeout=10)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            with z.open('eurofxref-hist.csv') as f:
                df = pd.read_csv(f)

            df.columns = df.columns.str.strip()
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df = df.set_index('Date')
            self.rates = df.to_dict(orient='index')
            print(f"  -> Success! Loaded rates for {len(self.rates)} days.")
        except Exception as e:
            print(f"  [CRITICAL ERROR] Could not load ECB rates: {e}")
            self.rates = {}

    def get_rate(self, date_str, currency):
        if currency == 'EUR': return 1.0, date_str, 1.0
        try:
            target_obj = pd.to_datetime(date_str)
        except:
            return None, None, None

        for i in range(5):
            check_date_str = (target_obj - timedelta(days=i)).strftime('%Y-%m-%d')
            if check_date_str in self.rates:
                rate_row = self.rates[check_date_str]
                if currency in rate_row:
                    raw_rate = rate_row[currency]
                    if not pd.isna(raw_rate) and raw_rate != 0:
                        return 1 / float(raw_rate), check_date_str, raw_rate
        return None, None, None


def find_isin_online(ticker):
    if ticker in MANUAL_ISIN_MAP:
        return MANUAL_ISIN_MAP[ticker]
    try:
        t = yf.Ticker(ticker)
        isin = t.isin
        if isin and isin != '-' and len(isin) > 0:
            return isin
    except:
        pass
    return None


def clean_number(value):
    if pd.isna(value) or value == '': return 0.0
    if isinstance(value, (float, int)): return float(value)
    clean = str(value).replace('$', '').replace('€', '').replace('£', '').replace(',', '').strip()
    clean = clean.replace('\xa0', '')  # Remove non-breaking spaces
    try:
        return float(clean)
    except:
        return 0.0


def clean_isin_str(value):
    """Returns empty string '' instead of None to prevent NaN issues."""
    if pd.isna(value): return ''
    s = str(value).strip()
    if s.lower() in ['nan', 'none', '', '-']: return ''
    return s


# --- PROCESSORS ---

def process_revolut(file_path, converter, audit_log, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path} (Revolut)...")

    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()

    for idx, row in df.iterrows():
        r_type = str(row.get('Type', '')).upper()
        ticker = row.get('Ticker', '') or row.get('Symbol', '')

        trans_type = ''
        if r_type in ['BUY', 'MARKET BUY']:
            trans_type = 'BUY'
        elif r_type in ['SELL', 'MARKET SELL']:
            trans_type = 'SELL'
        elif r_type in ['DIVIDEND', 'DIV']:
            trans_type = 'DIV'
        elif 'INTEREST' in r_type or 'SAVINGS' in str(row.get('Description', '')).upper():
            trans_type = 'INTEREST'

        if not trans_type: continue
        if trans_type in ['BUY', 'SELL', 'DIV'] and not ticker: continue

        raw_date = row.get('Date') or row.get('Completed Date')
        try:
            date_str = pd.to_datetime(raw_date).strftime('%Y-%m-%d')
        except:
            continue

        qty = row.get('Quantity')
        if pd.isna(qty): qty = row.get('Shares')
        quantity = clean_number(qty)

        amt = row.get('Total Amount')
        if pd.isna(amt): amt = row.get('Amount')
        if pd.isna(amt): amt = row.get('Value')
        if pd.isna(amt): amt = row.get('Net Amount')
        amount = abs(clean_number(amt))

        currency = str(row.get('Currency', 'USD')).strip()

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
            'Quantity': quantity,
            'TotalValueEUR': final_eur,
            'ISIN': clean_isin_str(row.get('ISIN')),
            'Name': ticker if ticker else 'Interest',
            'TaxPaidEUR': 0
        })
    return rows


def process_t212(file_path, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path} (Trading212)...")

    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()

    for idx, row in df.iterrows():
        action = str(row.get('Action', '')).lower()

        trans_type = ''
        if 'buy' in action:
            trans_type = 'BUY'
        elif 'sell' in action:
            trans_type = 'SELL'
        elif 'dividend' in action:
            trans_type = 'DIV'
        elif 'interest' in action:
            trans_type = 'INTEREST'
        elif 'lending' in action:
            trans_type = 'INTEREST'  # Treat lending interest as interest
        else:
            continue

        # --- FIX: Safe Ticker for Interest ---
        # If it's interest, Ticker is likely empty. We force it to 'CASH'.
        ticker = row.get('Ticker', '')
        if trans_type == 'INTEREST' and (pd.isna(ticker) or ticker == ''):
            ticker = 'CASH'

        date_str = pd.to_datetime(row.get('Time')).strftime('%Y-%m-%d')

        total = clean_number(row.get('Total (EUR)', 0))
        if total == 0:
            # Fallback calculation if Total (EUR) is missing
            raw_total = clean_number(row.get('Total', 0))
            # Some T212 files don't have 'Exchange rate', assume 1.0 or user currency
            fx = clean_number(row.get('Exchange rate', 1))
            # If FX is 0 (bad data), assume 1
            if fx == 0: fx = 1.0
            total = raw_total * fx

        rows.append({
            'Source': 'T212',
            'Date': date_str,
            'Type': trans_type,
            'Ticker': ticker,
            'Quantity': clean_number(row.get('No. of shares', 0)),
            'TotalValueEUR': abs(total),
            # Force empty string for ISIN to avoid NaN
            'ISIN': clean_isin_str(row.get('ISIN')),
            'Name': row.get('Name', '') or 'Trading212 Interest',
            'TaxPaidEUR': abs(clean_number(row.get('Withholding tax (EUR)', 0)))
        })
    return rows


def process_ibkr(file_path, converter, audit_log, skipped_log):
    return []


def main():
    converter = CurrencyConverter()
    audit_log = []
    skipped_log = []
    all_rows = []

    # 1. Load Data
    all_rows.extend(process_revolut(REVOLUT_FILE, converter, audit_log, skipped_log))
    all_rows.extend(process_t212(T212_FILE, skipped_log))
    all_rows.extend(process_ibkr(IBKR_FILE, converter, audit_log, skipped_log))

    df = pd.DataFrame(all_rows)

    # 2. INTELLIGENT ISIN FILL
    # Note: We filter for rows where Ticker is NOT 'CASH' so we don't try to find ISINs for money.
    print("\n--- INTELLIGENT ISIN FILL ---")
    if not df.empty and 'ISIN' in df.columns:
        # Fill Forward/Backward for STOCKS only
        df['ISIN'] = df.groupby('Ticker')['ISIN'].transform(lambda x: x.ffill().bfill())

        # Find Missing ISINs (Exclude CASH/Interest)
        missing_mask = (df['ISIN'] == '') & (df['Type'].isin(['BUY', 'SELL'])) & (df['Ticker'] != 'CASH')
        missing_tickers = df.loc[missing_mask, 'Ticker'].unique()

        if len(missing_tickers) > 0:
            print(f"Found {len(missing_tickers)} tickers missing ISINs. Searching...")
            for ticker in missing_tickers:
                found = find_isin_online(ticker)
                if found:
                    df.loc[df['Ticker'] == ticker, 'ISIN'] = found
                else:
                    skipped_log.append({
                        'Source': 'ISIN_CHECK',
                        'Row': 'All',
                        'Reason': f'Web ISIN failed for {ticker}',
                        'RawData': ticker
                    })

    # 3. Save
    df.sort_values(by='Date', inplace=True)

    # Ensure all Nulls are empty strings in final output for cleanliness
    df.fillna('', inplace=True)

    keys = ['Source', 'Date', 'Type', 'Ticker', 'ISIN', 'Name', 'Quantity', 'TotalValueEUR', 'TaxPaidEUR']
    df[keys].to_csv(MASTER_FILE, index=False)

    if audit_log: pd.DataFrame(audit_log).to_csv(AUDIT_FILE, index=False)
    if skipped_log: pd.DataFrame(skipped_log).to_csv(SKIPPED_FILE, index=False)

    print(f"\nDONE! Checked {len(df)} rows.")


if __name__ == "__main__":
    main()