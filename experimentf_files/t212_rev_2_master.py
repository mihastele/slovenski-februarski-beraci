import csv
import os
from datetime import datetime

# --- CONFIGURATION ---
# Rename your downloaded files to match these, or change these names:
T212_FILE = '../trading212.csv'
REVOLUT_FILE = '../revolut.csv'
OUTPUT_FILE = '../master_data.csv'


# ---------------------

def parse_date(date_str):
    """Attempts to parse date from various formats into YYYY-MM-DD"""
    formats = [
        '%Y-%m-%d', '%Y-%m-%d %H:%M:%S',  # Standard ISO
        '%d/%m/%Y', '%d/%m/%Y %H:%M:%S',  # European
        '%m/%d/%Y', '%m/%d/%Y %H:%M:%S',  # US
        '%d.%m.%Y', '%d-%m-%Y'  # Other
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str.split(' ')[0], fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return date_str  # Return original if fail


def clean_number(value):
    """Removes currency symbols and converts to float"""
    if not value: return 0.0
    clean = str(value).replace('$', '').replace('€', '').replace('£', '').replace(',', '')
    try:
        return float(clean)
    except ValueError:
        return 0.0


def process_trading212(file_path):
    data = []
    if not os.path.exists(file_path):
        print(f"Skipping {file_path} (File not found)")
        return data

    print(f"Processing {file_path}...")
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            action = row.get('Action', '').lower()

            # Map Types
            type_mapped = ''
            if 'buy' in action:
                type_mapped = 'BUY'
            elif 'sell' in action:
                type_mapped = 'SELL'
            elif 'dividend' in action:
                type_mapped = 'DIV'
            else:
                continue  # Skip deposits/withdrawals

            # Get Value in EUR
            # Priority: Total (EUR) -> Total * Exchange rate -> Total
            total_eur = 0.0
            if row.get('Total (EUR)'):
                total_eur = clean_number(row.get('Total (EUR)'))
            elif row.get('Total') and row.get('Exchange rate'):
                total_eur = clean_number(row.get('Total')) * clean_number(row.get('Exchange rate'))
            else:
                total_eur = clean_number(row.get('Total'))

            # Tax Paid (for dividends)
            tax_paid = 0.0
            if 'Withholding tax (EUR)' in row and row['Withholding tax (EUR)']:
                tax_paid = clean_number(row['Withholding tax (EUR)'])
            elif 'Withholding tax' in row:
                tax_paid = clean_number(row['Withholding tax'])  # Warning: Might be USD

            entry = {
                'Ticker': row.get('Ticker', ''),
                'Date': parse_date(row.get('Time', '')),
                'Type': type_mapped,
                'Quantity': clean_number(row.get('No. of shares', 0)),
                'TotalValueEUR': abs(total_eur),  # Always positive
                'ISIN': row.get('ISIN', ''),
                'Name': row.get('Name', ''),
                'TaxPaidEUR': abs(tax_paid)
            }
            data.append(entry)
    return data


def process_revolut(file_path):
    data = []
    if not os.path.exists(file_path):
        print(f"Skipping {file_path} (File not found)")
        return data

    print(f"Processing {file_path}...")
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Revolut headers change often. We try standard ones.
            # Common headers: 'Type', 'Ticker', 'Quantity', 'Price per share', 'Total Amount', 'Currency'

            r_type = row.get('Type', '').upper()

            type_mapped = ''
            if r_type in ['BUY', 'MARKET BUY']:
                type_mapped = 'BUY'
            elif r_type in ['SELL', 'MARKET SELL']:
                type_mapped = 'SELL'
            elif r_type in ['DIVIDEND', 'DIV']:
                type_mapped = 'DIV'
            else:
                continue

            # Ticker often has " - Name" appended, or just Ticker
            ticker = row.get('Ticker', '') or row.get('Symbol', '')

            # Value
            amount = clean_number(row.get('Total Amount', 0))
            if amount == 0: amount = clean_number(row.get('Amount', 0))

            # Currency Check (Simple Warning)
            currency = row.get('Currency', '')
            if currency == 'USD':
                print(
                    f"  [WARNING] Revolut trade {ticker} on {row.get('Date')} is in USD. Please convert manually in CSV.")

            entry = {
                'Ticker': ticker,
                'Date': parse_date(row.get('Date', '') or row.get('Completed Date', '')),
                'Type': type_mapped,
                'Quantity': clean_number(row.get('Quantity', 0)),
                'TotalValueEUR': abs(amount),
                'ISIN': '',  # Revolut CSV often lacks ISIN
                'Name': ticker,  # Fallback
                'TaxPaidEUR': 0  # Revolut CSV often lacks tax details clearly
            }
            data.append(entry)
    return data


def main():
    # 1. Process files
    t212_data = process_trading212(T212_FILE)
    rev_data = process_revolut(REVOLUT_FILE)

    all_data = t212_data + rev_data

    # 2. Sort by Date
    all_data.sort(key=lambda x: x['Date'])

    # 3. Write to Master CSV
    headers = ['Date', 'Type', 'Ticker', 'ISIN', 'Name', 'Quantity', 'TotalValueEUR', 'TaxPaidEUR']

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(all_data)

    print(f"\nSuccess! merged {len(all_data)} rows into '{OUTPUT_FILE}'.")
    print("IMPORTANT: Open the file and check 'TotalValueEUR' for Revolut rows (ensure they are EUR, not USD).")


if __name__ == "__main__":
    main()