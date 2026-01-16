import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom
from collections import defaultdict

# --- CONFIGURATION ---
INPUT_FILE = 'data.csv'
OUTPUT_FILE = 'Doh-KDVP.xml'
TAX_YEAR = 2025  # The year you are reporting for


# ---------------------

def create_xml(transactions):
    # Root Envelope
    envelope = ET.Element('Envelope', xmlns="http://edavki.durs.si/Documents/Schemas/Doh_KDVP_9.xsd")
    envelope.set('xmlns:edp', "http://edavki.durs.si/Documents/Schemas/EDP-Common-1.xsd")

    # Header (Minimal required)
    header = ET.SubElement(envelope, 'Header')
    taxpayer = ET.SubElement(header, 'taxpayer')
    # Note: You usually fill your personal Tax ID (Davčna) inside eDavki after import,
    # or you can add <taxNumber>12345678</taxNumber> here.

    ET.SubElement(header, 'Workflow')

    # Body
    body = ET.SubElement(envelope, 'body')
    doh_kdvp = ET.SubElement(body, 'Doh_KDVP')

    # Document Header
    kdvp = ET.SubElement(doh_kdvp, 'KDVP')
    doc_header = ET.SubElement(kdvp, 'Dokument')
    glava = ET.SubElement(doc_header, 'Glava')
    ET.SubElement(glava, 'Obdobje').text = str(TAX_YEAR)
    ET.SubElement(glava, 'VrstaPoro').text = 'O'  # O = Original

    # Process each Ticker
    for ticker, trades in transactions.items():
        kdvp_item = ET.SubElement(kdvp, 'KDVPItem')

        # Inventory Sheet (PopisniList)
        popisni_list = ET.SubElement(kdvp_item, 'PopisniList')
        ET.SubElement(popisni_list, 'Naziv').text = ticker  # Stock Name/Symbol
        ET.SubElement(popisni_list, 'Isin').text = ticker  # ISIN or Ticker

        # Split into Buys (Pridobitve) and Sells (Odsvojitve)
        pridobitve = ET.SubElement(popisni_list, 'Pridobitve')
        odsvojitve = ET.SubElement(popisni_list, 'Odsvojitve')

        for trade in trades:
            date = trade['Date']
            qty = trade['Quantity']
            val = trade['TotalValueEUR']

            if trade['Type'].upper() == 'BUY':
                row = ET.SubElement(pridobitve, 'Pridobitev')
                ET.SubElement(row, 'DatumPridobitve').text = date
                ET.SubElement(row, 'Kolicina').text = f"{float(qty):.4f}"
                ET.SubElement(row, 'NabavnaVrednost').text = f"{float(val):.4f}"
                ET.SubElement(row, 'NacinPridobitve').text = "A"  # A = Purchase

            elif trade['Type'].upper() == 'SELL':
                row = ET.SubElement(odsvojitve, 'Odsvojitev')
                ET.SubElement(row, 'DatumOdsvojitve').text = date
                ET.SubElement(row, 'Kolicina').text = f"{float(qty):.4f}"
                ET.SubElement(row, 'VrednostObOdsvojitvi').text = f"{float(val):.4f}"
                ET.SubElement(row, 'NacinOdsvojitve').text = "A"  # A = Sale

    return envelope


def main():
    # 1. Read CSV and Group by Ticker
    transactions = defaultdict(list)

    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                transactions[row['Ticker']].append(row)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found. Please create it first.")
        return

    # 2. Sort transactions by date for each ticker
    for ticker in transactions:
        transactions[ticker].sort(key=lambda x: x['Date'])

    # 3. Generate XML
    xml_structure = create_xml(transactions)

    # 4. Save to file (Pretty Print)
    xml_str = minidom.parseString(ET.tostring(xml_structure)).toprettyxml(indent="  ")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(xml_str)

    print(f"Success! '{OUTPUT_FILE}' generated. You can now import this into eDavki.")


if __name__ == "__main__":
    main()