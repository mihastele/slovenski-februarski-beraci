import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
import pandas as pd
from datetime import datetime

# --- CONFIGURATION ---
T212_FILE = 'trading212.csv'
REVOLUT_FILE = 'revolut.csv'
OUTPUT_FILE = 'Doh-Obr.xml'
TAX_YEAR = 2025


# --- INTEREST CODES (FURS) ---
# Code 1: Bank Deposits (EU/Slovenia) -> 1000€ tax-free limit applies.
# Code 2: Bank Deposits (Non-EU) -> 1000€ tax-free limit applies.
# Code 3: Other Interest (Brokers, Peer-to-Peer, Bonds) -> Fully Taxed (25%).
#
# RULE OF THUMB:
# Revolut (Bank UAB) -> Code 1 (Lithuania is EU)
# Trading212 -> Code 3 (It is a Broker, not a Bank, even if they put money in banks)
# ---------------------

def clean_number(value):
    if not value: return 0.0
    if isinstance(value, float) or isinstance(value, int): return float(value)
    clean = str(value).replace('$', '').replace('€', '').replace('£', '').replace(',', '')
    try:
        return float(clean)
    except ValueError:
        return 0.0


def generate_xml(interest_items):
    envelope = ET.Element('Envelope', xmlns="http://edavki.durs.si/Documents/Schemas/Doh_Obr_2.xsd")
    envelope.set('xmlns:edp', "http://edavki.durs.si/Documents/Schemas/EDP-Common-1.xsd")

    header = ET.SubElement(envelope, 'Header')
    ET.SubElement(header, 'taxpayer')
    ET.SubElement(header, 'Workflow')

    body = ET.SubElement(envelope, 'body')
    doh_obr = ET.SubElement(body, 'Doh_Obr')
    ET.SubElement(doh_obr, 'Obdobje').text = str(TAX_YEAR)

    obresti_list = ET.SubElement(doh_obr, 'Obresti')

    for item in interest_items:
        row = ET.SubElement(obresti_list, 'ObrestiItem')
        ET.SubElement(row, 'DatumPrejetja').text = item['Date']
        ET.SubElement(row, 'VrstaObresti').text = item['Code']
        ET.SubElement(row, 'Opis').text = item['Description']
        ET.SubElement(row, 'Znesek').text = f"{item['Amount']:.2f}"
        ET.SubElement(row, 'Drzava').text = item['Country']
        ET.SubElement(row, 'TujDavek').text = f"{item['TaxPaid']:.2f}"

        # Only set country source if tax was paid, otherwise optional/same
        if item['TaxPaid'] > 0:
            ET.SubElement(row, 'DrzavaVir').text = item['Country']

    return envelope


def get_revolut_interest():
    items = []
    if not os.path.exists(REVOLUT_FILE): return items

    print("Scanning Revolut for Interest...")
    df = pd.read_csv(REVOLUT_FILE)

    for _, row in df.iterrows():
        # Revolut interest often labelled as "Savings Interest" or type "INTEREST"
        r_type = str(row.get('Type', '')).upper()
        desc = str(row.get('Description', '')).upper()

        is_interest = False
        if r_type == 'INTEREST': is_interest = True
        if 'INTEREST' in desc: is_interest = True
        if 'SAVINGS' in desc and r_type == 'DIVIDEND': is_interest = True  # Flexible accounts

        if is_interest:
            # Parse Date
            raw_date = row.get('Date', '') or row.get('Completed Date', '')
            try:
                date_str = pd.to_datetime(raw_date).strftime('%Y-%m-%d')
            except:
                continue

            amount = clean_number(row.get('Total Amount', 0))
            if amount == 0: amount = clean_number(row.get('Amount', 0))

            # Simple USD/GBP to EUR conversion (Using fixed estimate if not in EUR)
            # ideally, use the super_convert logic, but for small interest,
            # approximate is often acceptable or you can manually fix.
            # Assuming EUR for simplicity or already converted.
            currency = row.get('Currency', 'EUR')
            if currency != 'EUR':
                print(f"  [!] Revolut Interest on {date_str} is in {currency}. Please check rate.")

            if amount > 0:
                items.append({
                    'Date': date_str,
                    'Code': '1',  # EU Bank Deposit (Revolut UAB)
                    'Description': 'Revolut Savings Interest',
                    'Amount': amount,
                    'Country': 'LT',  # Lithuania
                    'TaxPaid': 0.0  # Usually 0 for Revolut
                })
    return items


def get_t212_interest():
    items = []
    if not os.path.exists(T212_FILE): return items

    print("Scanning Trading212 for Interest...")
    df = pd.read_csv(T212_FILE)

    for _, row in df.iterrows():
        action = str(row.get('Action', '')).lower()

        # Catch "Interest on cash" and "Lending interest"
        if 'interest' in action:
            date_str = pd.to_datetime(row.get('Time')).strftime('%Y-%m-%d')

            # Use EUR columns
            total = clean_number(row.get('Total (EUR)', 0))
            if total == 0:
                total = clean_number(row.get('Total', 0))  # Fallback

            tax = clean_number(row.get('Withholding tax (EUR)', 0))

            items.append({
                'Date': date_str,
                'Code': '3',  # Other Interest (Broker) - Fully Taxed
                'Description': 'Trading212 Interest',
                'Amount': total,
                'Country': 'GB',  # Usually GB or CY (Cyprus) depending on account
                'TaxPaid': tax
            })
    return items


def main():
    all_interest = []

    # 1. Fetch Data
    all_interest.extend(get_revolut_interest())
    all_interest.extend(get_t212_interest())

    if not all_interest:
        print("No interest found in CSV files.")
        return

    # 2. Sort by date
    all_interest.sort(key=lambda x: x['Date'])

    # 3. Generate XML
    xml_structure = generate_xml(all_interest)
    xml_str = minidom.parseString(ET.tostring(xml_structure)).toprettyxml(indent="  ")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(xml_str)

    print(f"\nSuccess! Generated '{OUTPUT_FILE}' with {len(all_interest)} entries.")
    print("Import this into eDavki as 'Doh-Obr' (Napoved za odmero dohodnine od obresti).")


if __name__ == "__main__":
    main()