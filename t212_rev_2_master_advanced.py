import csv
import os
import requests
import zipfile
import io
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict

# --- CONFIGURATION ---
T212_FILE = 'trading212.csv'
REVOLUT_FILE = 'revolut.csv'
MASTER_FILE = 'master_data.csv'
AUDIT_FILE = 'conversion_audit.csv'
ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"


# ---------------------

class CurrencyConverter:
    def __init__(self):
        self.rates = {}
        self.load_ecb_rates()

    def load_ecb_rates(self):
        """Downloads official ECB history and loads into memory."""
        print(f"Downloading official exchange rates from ECB...")
        try:
            r = requests.get(ECB_URL)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            # The zip contains a single file 'eurofxref-hist.csv'
            with z.open('eurofxref-hist.csv') as f:
                df = pd.read_csv(f)

            # Create a lookup dictionary: { 'YYYY-MM-DD': { 'USD': 1.05, ... } }
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
            self.rates = df.to_dict(orient='index')
            print("Exchange rates loaded successfully.")
        except Exception as e:
            print(f"CRITICAL ERROR loading ECB rates: {e}")

    def get_rate(self, date_str, currency):
        """
        Returns EUR value of 1 unit of currency.
        ECB provides 1 EUR = X USD. So to get EUR from USD, we divide 1 / Rate.
        Handles weekends by looking back up to 5 days.
        """
        if currency == 'EUR': return 1.0

        target_date = pd.to_datetime(date_str)

        # Look back up to 5 days for a valid rate (e.g., weekends/holidays)
        for i in range(5):
            check_date = target_date - timedelta(days=i)
            # Keys in ECB dict are Timestamps, convert to match
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
        # yfinance often hides ISIN in .isin property
        if hasattr(t, 'isin') and t.isin and t.isin != '-':
            return t.isin

        # Sometimes it's in info (slower)
        # info = t.info
        # return info.get('isin', '')
    except Exception:
        pass
    return ''


def clean_number(value):
    if not value: return 0.0
    if isinstance(value, float) or isinstance(value, int): return float(value)
    clean = str(value).replace('$', '').replace('€', '').replace('£', '').replace(',', '')
    try:
        return float(clean)
    except ValueError:
        return 0.0


def process_revolut_row(row, converter, audit_log):
    ticker = row.get('Ticker', '') or row.get('Symbol', '')

    # Parse Date
    raw_date = row.get('Date', '') or row.get('Completed Date', '')
    try:
        # Revolut standard: 2024-01-15T14:30:00Z
        dt = pd.to_datetime(raw_date)
        date_str = dt.strftime('%Y-%m-%d')
    except:
        print(f"Skipping row with bad date: {raw_date}")
        return None

    # Detect Type
    r_type = row.get('Type', '').upper()
    if r_type in ['BUY', 'MARKET BUY']:
        trans_type = 'BUY'
    elif r_type in ['SELL', 'MARKET SELL']:
        trans_type = 'SELL'
    elif r_type in ['DIVIDEND', 'DIV']:
        trans_type = 'DIV'
    else:
        return None

    # Amounts and Currency
    qty = clean_number(row.get('Quantity', 0))
    amount = clean_number(row.get('Total Amount', 0))
    if amount == 0: amount = clean_number(row.get('Amount', 0))
    amount = abs(amount)

    currency = row.get('Currency', 'USD')  # Default to USD if missing

    # CONVERSION MAGIC
    final_eur = amount
    rate_used = 1.0
    rate_date = date_str

    if currency != 'EUR':
        rate, found_date, raw_ecb = converter.get_rate(date_str, currency)
        if rate:
            final_eur = amount * rate
            rate_used = rate
            rate_date = found_date
            audit_log.append({
                'Date': date_str,
                'Ticker': ticker,
                'OriginalAmount': amount,
                'Currency': currency,
                'RateDateUsed': found_date,
                'ECB_Rate_1EUR_to_X': raw_ecb,
                'AppliedRate_to_EUR': rate,
                'FinalEUR': final_eur
            })
        else:
            print(f"  [WARNING] Could not find ECB rate for {currency} on {date_str}")

    return {
        'Date': date_str,
        'Type': trans_type,
        'Ticker': ticker,
        'Quantity': qty,
        'TotalValueEUR': final_eur,
        'ISIN': '',  # Will fill later
        'Name': ticker,
        'TaxPaidEUR': 0  # Revolut CSV usually weak on this
    }


def process_trading212_row(row):
    # (Same logic as before, T212 usually provides EUR)
    action = row.get('Action', '').lower()
    if 'buy' in action:
        t_type = 'BUY'
    elif 'sell' in action:
        t_type = 'SELL'
    elif 'dividend' in action:
        t_type = 'DIV'
    else:
        return None

    date_str = pd.to_datetime(row.get('Time')).strftime('%Y-%m-%d')

    # Priority to EUR column
    total = clean_number(row.get('Total (EUR)', 0))
    if total == 0:
        total = clean_number(row.get('Total', 0)) * clean_number(row.get('Exchange rate', 1))

    tax = clean_number(row.get('Withholding tax (EUR)', 0))

    return {
        'Date': date_str,
        'Type': t_type,
        'Ticker': row.get('Ticker', ''),
        'Quantity': clean_number(row.get('No. of shares', 0)),
        'TotalValueEUR': abs(total),
        'ISIN': row.get('ISIN', ''),
        'Name': row.get('Name', ''),
        'TaxPaidEUR': abs(tax)
    }


def main():
    converter = CurrencyConverter()
    audit_log = []
    all_rows = []

    # 1. Process Revolut
    if os.path.exists(REVOLUT_FILE):
        print(f"Processing {REVOLUT_FILE}...")
        df = pd.read_csv(REVOLUT_FILE)
        for _, row in df.iterrows():
            item = process_revolut_row(row, converter, audit_log)
            if item: all_rows.append(item)

    # 2. Process Trading212
    if os.path.exists(T212_FILE):
        print(f"Processing {T212_FILE}...")
        df = pd.read_csv(T212_FILE)
        for _, row in df.iterrows():
            item = process_trading212_row(row)
            if item: all_rows.append(item)

    # 3. ISIN Lookup & Deduplication
    print("Checking for missing ISINs...")

    # Create a cache of found ISINs to avoid re-querying the same ticker
    ticker_isin_map = {}

    # First pass: collect existing ISINs from T212 data
    for r in all_rows:
        if r['ISIN'] and r['Ticker']:
            ticker_isin_map[r['Ticker']] = r['ISIN']

    # Second pass: fill missing
    for r in all_rows:
        if not r['ISIN']:
            if r['Ticker'] in ticker_isin_map:
                r['ISIN'] = ticker_isin_map[r['Ticker']]
            else:
                # Go Online!
                found = find_isin_online(r['Ticker'])
                if found:
                    r['ISIN'] = found
                    ticker_isin_map[r['Ticker']] = found
                else:
                    print(f"  [!] Could not find ISIN for {r['Ticker']}. You may need to add it manually.")

    # 4. Save Master File
    all_rows.sort(key=lambda x: x['Date'])
    keys = ['Date', 'Type', 'Ticker', 'ISIN', 'Name', 'Quantity', 'TotalValueEUR', 'TaxPaidEUR']

    with open(MASTER_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_rows)

    # 5. Save Audit Log
    if audit_log:
        audit_keys = ['Date', 'Ticker', 'OriginalAmount', 'Currency', 'RateDateUsed', 'ECB_Rate_1EUR_to_X',
                      'AppliedRate_to_EUR', 'FinalEUR']
        with open(AUDIT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=audit_keys)
            writer.writeheader()
            writer.writerows(audit_log)
        print(f"\n[AUDIT] Generated '{AUDIT_FILE}' with {len(audit_log)} conversion records.")

    print(f"\n[SUCCESS] Generated '{MASTER_FILE}'. Now run the XML generator script.")


if __name__ == "__main__":
    main()