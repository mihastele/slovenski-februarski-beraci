import csv
import os
import requests
import zipfile
import io
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

# --- CONFIGURATION ---
T212_FILE = '../trading212.csv'
REVOLUT_FILE = '../revolut.csv'
MASTER_FILE = '../master_data.csv'
AUDIT_FILE = '../conversion_audit.csv'  # Proof of currency rates
SKIPPED_FILE = '../skipped_rows.csv'  # Proof of what was ignored
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
            print("Exchange rates loaded successfully.")
        except Exception as e:
            print(f"CRITICAL ERROR loading ECB rates: {e}")

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
    print(f"  ... Searching online for ISIN: {ticker}")
    try:
        t = yf.Ticker(ticker)
        if hasattr(t, 'isin') and t.isin and t.isin != '-':
            return t.isin
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


# --- PROCESSORS ---

def process_revolut(file_path, converter, audit_log, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows

    print(f"Processing {file_path}...")
    df = pd.read_csv(file_path)

    for idx, row in df.iterrows():
        # Identify Columns (Revolut changes formats often)
        r_type = str(row.get('Type', '')).upper()
        ticker = row.get('Ticker', '') or row.get('Symbol', '')

        # 1. CHECK: Is this a stock trade?
        if not ticker or pd.isna(ticker):
            skipped_log.append({'Source': 'Revolut', 'RowIndex': idx, 'Type': r_type,
                                'Reason': 'No Ticker (Likely Cash/Card Transaction)'})
            continue

        valid_types = {
            'BUY': 'BUY', 'MARKET BUY': 'BUY',
            'SELL': 'SELL', 'MARKET SELL': 'SELL',
            'DIVIDEND': 'DIV', 'DIV': 'DIV'
        }

        if r_type not in valid_types:
            skipped_log.append({'Source': 'Revolut', 'RowIndex': idx, 'Type': r_type,
                                'Reason': 'Non-Taxable Type (Deposit/Transfer/Custody Fee)'})
            continue

        trans_type = valid_types[r_type]

        # 2. Date Parsing
        raw_date = row.get('Date', '') or row.get('Completed Date', '')
        try:
            date_str = pd.to_datetime(raw_date).strftime('%Y-%m-%d')
        except:
            skipped_log.append(
                {'Source': 'Revolut', 'RowIndex': idx, 'Type': r_type, 'Reason': f'Invalid Date: {raw_date}'})
            continue

        # 3. Currency Conversion
        qty = clean_number(row.get('Quantity', 0))
        amount = clean_number(row.get('Total Amount', 0))
        if amount == 0: amount = clean_number(row.get('Amount', 0))
        amount = abs(amount)
        currency = row.get('Currency', 'USD')

        final_eur = amount
        if currency != 'EUR':
            rate, found_date, raw_ecb = converter.get_rate(date_str, currency)
            if rate:
                final_eur = amount * rate
                audit_log.append({
                    'Date': date_str, 'Ticker': ticker, 'OriginalAmount': amount, 'Currency': currency,
                    'RateDateUsed': found_date, 'ECB_Rate': raw_ecb, 'FinalEUR': final_eur
                })
            else:
                print(f"  [WARNING] No ECB rate for {currency} on {date_str}")

        rows.append({
            'Date': date_str, 'Type': trans_type, 'Ticker': ticker,
            'Quantity': qty, 'TotalValueEUR': final_eur,
            'ISIN': '', 'Name': ticker, 'TaxPaidEUR': 0
        })
    return rows


def process_trading212(file_path, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows

    print(f"Processing {file_path}...")
    df = pd.read_csv(file_path)

    for idx, row in df.iterrows():
        action = str(row.get('Action', '')).lower()

        # 1. FILTER: Only Buy, Sell, Dividend
        if 'deposit' in action:
            skipped_log.append({'Source': 'T212', 'RowIndex': idx, 'Type': action, 'Reason': 'Deposit (Not Taxable)'})
            continue
        if 'withdrawal' in action:
            skipped_log.append(
                {'Source': 'T212', 'RowIndex': idx, 'Type': action, 'Reason': 'Withdrawal (Not Taxable)'})
            continue
        if 'interest' in action:
            skipped_log.append({'Source': 'T212', 'RowIndex': idx, 'Type': action,
                                'Reason': 'Cash Interest (Report via Doh-Obr, not Stock)'})
            continue

        trans_type = ''
        if 'buy' in action:
            trans_type = 'BUY'
        elif 'sell' in action:
            trans_type = 'SELL'
        elif 'dividend' in action:
            trans_type = 'DIV'
        else:
            skipped_log.append({'Source': 'T212', 'RowIndex': idx, 'Type': action, 'Reason': 'Unknown Action'})
            continue

        # 2. Extract Data
        try:
            date_str = pd.to_datetime(row.get('Time')).strftime('%Y-%m-%d')

            # Prefer 'Total (EUR)' if available
            total = clean_number(row.get('Total (EUR)', 0))
            if total == 0:
                total = clean_number(row.get('Total', 0)) * clean_number(row.get('Exchange rate', 1))

            tax = clean_number(row.get('Withholding tax (EUR)', 0))

            rows.append({
                'Date': date_str, 'Type': trans_type,
                'Ticker': row.get('Ticker', ''),
                'Quantity': clean_number(row.get('No. of shares', 0)),
                'TotalValueEUR': abs(total),
                'ISIN': row.get('ISIN', ''),
                'Name': row.get('Name', ''),
                'TaxPaidEUR': abs(tax)
            })
        except Exception as e:
            skipped_log.append({'Source': 'T212', 'RowIndex': idx, 'Type': action, 'Reason': f'Error Parsing Row: {e}'})

    return rows


# --- MAIN ---

def main():
    converter = CurrencyConverter()
    audit_log = []
    skipped_log = []  # New Log for discarded rows

    # 1. Process
    rev_data = process_revolut(REVOLUT_FILE, converter, audit_log, skipped_log)
    t212_data = process_trading212(T212_FILE, skipped_log)

    all_data = rev_data + t212_data

    # 2. ISIN Cleanup (Deduplicated logic)
    print("Checking ISINs...")
    ticker_isin_map = {r['Ticker']: r['ISIN'] for r in all_data if r['ISIN']}

    for r in all_data:
        if not r['ISIN']:
            if r['Ticker'] in ticker_isin_map:
                r['ISIN'] = ticker_isin_map[r['Ticker']]
            else:
                found = find_isin_online(r['Ticker'])
                if found:
                    r['ISIN'] = found
                    ticker_isin_map[r['Ticker']] = found

    # 3. Save Master
    all_data.sort(key=lambda x: x['Date'])
    keys = ['Date', 'Type', 'Ticker', 'ISIN', 'Name', 'Quantity', 'TotalValueEUR', 'TaxPaidEUR']
    pd.DataFrame(all_data)[keys].to_csv(MASTER_FILE, index=False)

    # 4. Save Audit Logs
    if audit_log: pd.DataFrame(audit_log).to_csv(AUDIT_FILE, index=False)
    if skipped_log: pd.DataFrame(skipped_log).to_csv(SKIPPED_FILE, index=False)

    # 5. FINAL SUMMARY
    print("\n" + "=" * 30)
    print(f"DONE! Summary:")
    print(f"  - Rows Loaded from CSVs:  Unknown (Raw)")
    print(f"  - Master Rows Created:    {len(all_data)} (Saved in {MASTER_FILE})")
    print(f"  - Rows Skipped:           {len(skipped_log)} (Saved in {SKIPPED_FILE})")
    print("=" * 30)
    print(f"PLEASE CHECK '{SKIPPED_FILE}' to ensure no real trades were lost!")


if __name__ == "__main__":
    main()