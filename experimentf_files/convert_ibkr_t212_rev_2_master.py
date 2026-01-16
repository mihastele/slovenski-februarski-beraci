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
IBKR_FILE = 'ibkr.csv'  # Rename your IBKR Activity Statement to this
MASTER_FILE = '../master_data.csv'
AUDIT_FILE = '../conversion_audit.csv'
SKIPPED_FILE = '../skipped_rows.csv'
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
    try:
        t = yf.Ticker(ticker)
        if hasattr(t, 'isin') and t.isin and t.isin != '-': return t.isin
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


# --- IBKR PARSER ---
def process_ibkr(file_path, converter, audit_log, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path} (Interactive Brokers)...")

    # IBKR CSVs are messy. We look for the "Trades" section headers first.
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except:
        return rows

    # Find the header row for Trades
    header_map = {}
    data_lines = []

    for line in lines:
        parts = [p.strip().replace('"', '') for p in line.split(',')]
        if len(parts) < 2: continue

        # IBKR Format: "Trades", "Header", "DataDiscriminator", "Asset Category", ...
        if parts[0] == 'Trades' and parts[1] == 'Header':
            for i, col in enumerate(parts):
                header_map[col] = i
        elif parts[0] == 'Trades' and parts[1] == 'Data' and header_map:
            data_lines.append(parts)

    for idx, cols in enumerate(data_lines):
        try:
            # Map columns
            get_col = lambda name: cols[header_map[name]] if name in header_map and header_map[name] < len(cols) else ''

            asset_type = get_col('Asset Category')
            if asset_type != 'Stocks':
                skipped_log.append({'Source': 'IBKR', 'RowIndex': idx, 'Type': asset_type, 'Reason': 'Not a Stock'})
                continue

            ticker = get_col('Symbol')
            date_raw = get_col('Date/Time').split(',')[0]  # 2023-01-05
            qty = clean_number(get_col('Quantity'))
            proceeds = clean_number(get_col('Proceeds'))  # Net cash usually
            fee = clean_number(get_col('Comm/Fee'))  # Usually negative
            currency = get_col('Currency')

            # Determine Type
            # IBKR: Buy is Qty > 0, Sell is Qty < 0
            if qty > 0:
                trans_type = 'BUY'
                # Cost Basis = Abs(Proceeds) + Abs(Fee)
                raw_val = abs(proceeds) + abs(fee)
            else:
                trans_type = 'SELL'
                # Disposal Value = Abs(Proceeds) - Abs(Fee) (You get less money)
                # Note: 'Proceeds' in IBKR usually matches Price*Qty.
                # FURS expects: (Price*Qty) - Costs.
                # If IBKR 'Proceeds' is just Price*Qty, we subtract fee.
                raw_val = abs(proceeds) - abs(fee)

            # Currency Conversion
            final_eur = raw_val
            if currency != 'EUR':
                rate, found_date, raw_ecb = converter.get_rate(date_raw, currency)
                if rate:
                    final_eur = raw_val * rate
                    audit_log.append(
                        {'Date': date_raw, 'Ticker': ticker, 'OriginalAmount': raw_val, 'Currency': currency,
                         'RateDateUsed': found_date, 'ECB_Rate': raw_ecb, 'FinalEUR': final_eur})
                else:
                    print(f"  [!] IBKR: No rate for {currency} on {date_raw}")

            rows.append({
                'Date': date_raw, 'Type': trans_type, 'Ticker': ticker,
                'Quantity': abs(qty), 'TotalValueEUR': final_eur,
                'ISIN': get_col('ISIN') or '',  # IBKR often has ISIN
                'Name': get_col('Description') or ticker,
                'TaxPaidEUR': 0  # Taxes usually in separate section
            })
        except Exception as e:
            skipped_log.append({'Source': 'IBKR', 'RowIndex': idx, 'Type': 'Error', 'Reason': str(e)})

    return rows


# --- REVOLUT & T212 PROCESSORS (Same as before) ---
def process_revolut(file_path, converter, audit_log, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path}...")
    df = pd.read_csv(file_path)
    for idx, row in df.iterrows():
        r_type = str(row.get('Type', '')).upper()
        ticker = row.get('Ticker', '') or row.get('Symbol', '')
        if not ticker or pd.isna(ticker): continue

        valid = {'BUY': 'BUY', 'MARKET BUY': 'BUY', 'SELL': 'SELL', 'MARKET SELL': 'SELL', 'DIVIDEND': 'DIV',
                 'DIV': 'DIV'}
        if r_type not in valid:
            skipped_log.append({'Source': 'Revolut', 'RowIndex': idx, 'Type': r_type, 'Reason': 'Skipped Type'})
            continue

        raw_date = row.get('Date', '') or row.get('Completed Date', '')
        try:
            date_str = pd.to_datetime(raw_date).strftime('%Y-%m-%d')
        except:
            continue

        qty = clean_number(row.get('Quantity', 0))
        amount = abs(clean_number(row.get('Total Amount', 0) or row.get('Amount', 0)))
        currency = row.get('Currency', 'USD')

        final_eur = amount
        if currency != 'EUR':
            rate, found_date, raw_ecb = converter.get_rate(date_str, currency)
            if rate:
                final_eur = amount * rate
                audit_log.append({'Date': date_str, 'Ticker': ticker, 'OriginalAmount': amount, 'Currency': currency,
                                  'RateDateUsed': found_date, 'ECB_Rate': raw_ecb, 'FinalEUR': final_eur})

        rows.append(
            {'Date': date_str, 'Type': valid[r_type], 'Ticker': ticker, 'Quantity': qty, 'TotalValueEUR': final_eur,
             'ISIN': '', 'Name': ticker, 'TaxPaidEUR': 0})
    return rows


def process_t212(file_path, skipped_log):
    rows = []
    if not os.path.exists(file_path): return rows
    print(f"Processing {file_path}...")
    df = pd.read_csv(file_path)
    for idx, row in df.iterrows():
        action = str(row.get('Action', '')).lower()
        if 'buy' in action:
            t_type = 'BUY'
        elif 'sell' in action:
            t_type = 'SELL'
        elif 'dividend' in action:
            t_type = 'DIV'
        else:
            skipped_log.append({'Source': 'T212', 'RowIndex': idx, 'Type': action, 'Reason': 'Not Trade/Div'})
            continue

        date_str = pd.to_datetime(row.get('Time')).strftime('%Y-%m-%d')
        total = clean_number(row.get('Total (EUR)', 0))
        if total == 0: total = clean_number(row.get('Total', 0)) * clean_number(row.get('Exchange rate', 1))

        rows.append({
            'Date': date_str, 'Type': t_type, 'Ticker': row.get('Ticker', ''),
            'Quantity': clean_number(row.get('No. of shares', 0)), 'TotalValueEUR': abs(total),
            'ISIN': row.get('ISIN', ''), 'Name': row.get('Name', ''),
            'TaxPaidEUR': abs(clean_number(row.get('Withholding tax (EUR)', 0)))
        })
    return rows


def main():
    converter = CurrencyConverter()
    audit, skipped, all_rows = [], [], []

    all_rows.extend(process_revolut(REVOLUT_FILE, converter, audit, skipped))
    all_rows.extend(process_t212(T212_FILE, skipped))
    all_rows.extend(process_ibkr(IBKR_FILE, converter, audit, skipped))

    # ISIN Lookup
    print("Verifying ISINs...")
    existing_isins = {r['Ticker']: r['ISIN'] for r in all_rows if r['ISIN']}
    for r in all_rows:
        if not r['ISIN']:
            if r['Ticker'] in existing_isins:
                r['ISIN'] = existing_isins[r['Ticker']]
            else:
                found = find_isin_online(r['Ticker'])
                if found:
                    r['ISIN'] = found
                    existing_isins[r['Ticker']] = found

    # Save
    all_rows.sort(key=lambda x: x['Date'])
    keys = ['Date', 'Type', 'Ticker', 'ISIN', 'Name', 'Quantity', 'TotalValueEUR', 'TaxPaidEUR']
    pd.DataFrame(all_rows)[keys].to_csv(MASTER_FILE, index=False)
    if audit: pd.DataFrame(audit).to_csv(AUDIT_FILE, index=False)
    if skipped: pd.DataFrame(skipped).to_csv(SKIPPED_FILE, index=False)
    print(f"\n[DONE] Generated '{MASTER_FILE}'. Check '{SKIPPED_FILE}' for ignored rows.")


if __name__ == "__main__":
    main()