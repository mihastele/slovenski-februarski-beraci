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
# If yfinance cannot find it, add it here.
# Example: 'HPQ': 'US40434L1052'
MANUAL_ISIN_MAP = {
    # 'HPQ': 'US40434L1052',  # HP Inc often fails on auto-lookup
    # 'O': 'US7561091049',  # Realty Income
}


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
    """Checks Manual Map first, then Web."""
    # 1. Check Manual Map
    if ticker in MANUAL_ISIN_MAP:
        print(f"  [MANUAL MAP] Used hardcoded ISIN for {ticker}")
        return MANUAL_ISIN_MAP[ticker]

    # 2. Check Web
    print(f"  [WEB SEARCH] Looking up ISIN for: {ticker}...")
    try:
        t = yf.Ticker(ticker)
        isin = t.isin
        if isin and isin != '-' and len(isin) > 0:
            print(f"    -> FOUND: {isin}")
            return isin
    except:
        pass

    print(f"    -> FAILED")
    return None


def clean_number(value):
    if not value: return 0.0
    if isinstance(value, (float, int)): return float(value)
    clean = str(value).replace('$', '').replace('€', '').replace('£', '').replace(',', '')
    try:
        return float(clean)
    except:
        return 0.0


def clean_isin_str(value):
    """Normalize input to either a valid string or None (for Pandas)"""
    if pd.isna(value): return None
    s = str(value).strip()
    if s.lower() in ['nan', 'none', '', '-']: return None
    return s


# --- PROCESSORS ---

def process_revolut(file_path, converter, audit_log, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path} (Revolut)...")
    df = pd.read_csv(file_path)

    for idx, row in df.iterrows():
        r_type = str(row.get('Type', '')).upper()
        ticker = row.get('Ticker', '') or row.get('Symbol', '')

        # Filter types
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

        date_str = pd.to_datetime(row.get('Date') or row.get('Completed Date')).strftime('%Y-%m-%d')
        amount = abs(clean_number(row.get('Total Amount', 0) or row.get('Amount', 0)))
        currency = row.get('Currency', 'USD')

        # FX Conversion
        final_eur = amount
        if currency != 'EUR':
            rate, found_date, raw_ecb = converter.get_rate(date_str, currency)
            if rate:
                final_eur = amount * rate
                audit_log.append(
                    {'Date': date_str, 'Source': 'Revolut', 'Ticker': ticker, 'OrigAmount': amount, 'Curr': currency,
                     'RateUsed': raw_ecb, 'FinalEUR': final_eur})
            else:
                print(f"  [WARNING] No ECB rate for {currency}")

        rows.append({
            'Source': 'Revolut', 'Date': date_str, 'Type': trans_type,
            'Ticker': ticker if ticker else 'CASH',
            'Quantity': clean_number(row.get('Quantity', 0)),
            'TotalValueEUR': final_eur,
            'ISIN': clean_isin_str(row.get('ISIN')),  # Use clean_isin_str
            'Name': ticker if ticker else 'Interest',
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
        elif 'interest' in action:
            trans_type = 'INTEREST'
        elif 'lending' in action:
            trans_type = 'LENDING'
        else:
            continue

        total = clean_number(row.get('Total (EUR)', 0))
        if total == 0: total = clean_number(row.get('Total', 0)) * clean_number(row.get('Exchange rate', 1))

        rows.append({
            'Source': 'T212', 'Date': pd.to_datetime(row.get('Time')).strftime('%Y-%m-%d'),
            'Type': trans_type,
            'Ticker': row.get('Ticker', '') or 'CASH',
            'Quantity': clean_number(row.get('No. of shares', 0)),
            'TotalValueEUR': abs(total),
            'ISIN': clean_isin_str(row.get('ISIN')),
            'Name': row.get('Name', '') or 'Interest',
            'TaxPaidEUR': abs(clean_number(row.get('Withholding tax (EUR)', 0)))
        })
    return rows


def process_ibkr(file_path, converter, audit_log, skipped_log):
    # (Placeholder - add your IBKR logic if needed)
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

    # Convert to DataFrame immediately for smarter processing
    df = pd.DataFrame(all_rows)

    print("\n--- INTELLIGENT ISIN FILL ---")

    # 2. INTERNAL PROPAGATION (The "Effort" to self-heal)
    # If ANY row has an ISIN for 'HPQ', copy it to ALL 'HPQ' rows.
    # We group by Ticker and fill Forward (ffill) and Backward (bfill)
    if not df.empty and 'ISIN' in df.columns:
        print("Propagating known ISINs across all rows...")
        df['ISIN'] = df.groupby('Ticker')['ISIN'].transform(lambda x: x.ffill().bfill())

    # 3. WEB SEARCH for stubborn Nulls
    # Filter for Stock trades (BUY/SELL) that still have no ISIN
    if not df.empty:
        missing_mask = (df['ISIN'].isna()) & (df['Type'].isin(['BUY', 'SELL']))
        missing_tickers = df.loc[missing_mask, 'Ticker'].unique()

        if len(missing_tickers) > 0:
            print(f"Found {len(missing_tickers)} tickers still missing ISINs. Searching online...")

            for ticker in missing_tickers:
                found = find_isin_online(ticker)

                if found:
                    # UPDATE THE DATAFRAME for ALL instances of this ticker
                    df.loc[df['Ticker'] == ticker, 'ISIN'] = found
                else:
                    # Log failure
                    skipped_log.append({
                        'Source': 'ISIN_CHECK',
                        'Row': 'All',
                        'Reason': f'ISIN not found on Web for {ticker}',
                        'RawData': ticker
                    })

    # 4. Save
    df.sort_values(by='Date', inplace=True)
    keys = ['Source', 'Date', 'Type', 'Ticker', 'ISIN', 'Name', 'Quantity', 'TotalValueEUR', 'TaxPaidEUR']
    df[keys].to_csv(MASTER_FILE, index=False)

    if audit_log: pd.DataFrame(audit_log).to_csv(AUDIT_FILE, index=False)
    if skipped_log: pd.DataFrame(skipped_log).to_csv(SKIPPED_FILE, index=False)

    print("\n" + "=" * 30)
    print(f"DONE!")
    print(f"Master File: {MASTER_FILE}")
    if skipped_log:
        print(f"WARNING: Check {SKIPPED_FILE} for tickers where Web Search FAILED.")
    print("=" * 30)


if __name__ == "__main__":
    main()