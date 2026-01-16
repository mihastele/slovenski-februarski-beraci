import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom
from collections import defaultdict
import os

# --- CONFIGURATION ---
INPUT_FILE = '../master_data.csv'
FILE_KDVP = '../Doh-KDVP.xml'
FILE_DIV = '../Doh-Div.xml'
TAX_YEAR = 2025  # The year you are reporting for


# ---------------------

def format_decimal(value, precision=2):
    """Formats float to string with varying precision needed for XML"""
    try:
        return f"{float(value):.{precision}f}"
    except (ValueError, TypeError):
        return "0.00"


def get_country_from_isin(isin):
    """Extracts country code from ISIN (first 2 chars). Default to US if missing."""
    if isin and len(isin) >= 2:
        return isin[:2].upper()
    return "US"  # Fallback


def generate_kdvp(transactions):
    """Generates Capital Gains XML (Inventory Import)"""
    envelope = ET.Element('Envelope', xmlns="http://edavki.durs.si/Documents/Schemas/Doh_KDVP_9.xsd")
    envelope.set('xmlns:edp', "http://edavki.durs.si/Documents/Schemas/EDP-Common-1.xsd")

    header = ET.SubElement(envelope, 'Header')
    ET.SubElement(header, 'taxpayer')
    ET.SubElement(header, 'Workflow')

    body = ET.SubElement(envelope, 'body')
    doh_kdvp = ET.SubElement(body, 'Doh_KDVP')
    kdvp = ET.SubElement(doh_kdvp, 'KDVP')

    # Document Header
    doc_header = ET.SubElement(kdvp, 'Dokument')
    glava = ET.SubElement(doc_header, 'Glava')
    ET.SubElement(glava, 'Obdobje').text = str(TAX_YEAR)
    ET.SubElement(glava, 'VrstaPoro').text = 'O'

    # Group trades by Ticker
    grouped = defaultdict(list)
    for t in transactions:
        if t['Type'] in ['BUY', 'SELL']:
            grouped[t['Ticker']].append(t)

    # Create Inventory Sheets
    for ticker, trades in grouped.items():
        # Skip if ticker has no ISIN in any row, try to find one
        isin = next((t['ISIN'] for t in trades if t['ISIN']), ticker)
        name = next((t['Name'] for t in trades if t['Name']), ticker)

        kdvp_item = ET.SubElement(kdvp, 'KDVPItem')
        popisni_list = ET.SubElement(kdvp_item, 'PopisniList')
        ET.SubElement(popisni_list, 'Naziv').text = name
        ET.SubElement(popisni_list, 'Isin').text = isin

        pridobitve = ET.SubElement(popisni_list, 'Pridobitve')
        odsvojitve = ET.SubElement(popisni_list, 'Odsvojitve')

        trades.sort(key=lambda x: x['Date'])  # Sort by date

        for trade in trades:
            row = None
            if trade['Type'] == 'BUY':
                row = ET.SubElement(pridobitve, 'Pridobitev')
                ET.SubElement(row, 'DatumPridobitve').text = trade['Date']
                ET.SubElement(row, 'Kolicina').text = format_decimal(trade['Quantity'], 4)
                ET.SubElement(row, 'NabavnaVrednost').text = format_decimal(trade['TotalValueEUR'], 4)
                ET.SubElement(row, 'NacinPridobitve').text = "A"  # Purchase
            elif trade['Type'] == 'SELL':
                row = ET.SubElement(odsvojitve, 'Odsvojitev')
                ET.SubElement(row, 'DatumOdsvojitve').text = trade['Date']
                ET.SubElement(row, 'Kolicina').text = format_decimal(trade['Quantity'], 4)
                ET.SubElement(row, 'VrednostObOdsvojitvi').text = format_decimal(trade['TotalValueEUR'], 4)
                ET.SubElement(row, 'NacinOdsvojitve').text = "A"  # Sale

    return envelope


def generate_dividends(transactions):
    """Generates Dividends XML (Document Import)"""
    envelope = ET.Element('Envelope', xmlns="http://edavki.durs.si/Documents/Schemas/Doh_Div_2.xsd")
    envelope.set('xmlns:edp', "http://edavki.durs.si/Documents/Schemas/EDP-Common-1.xsd")

    header = ET.SubElement(envelope, 'Header')
    ET.SubElement(header, 'taxpayer')
    ET.SubElement(header, 'Workflow')

    body = ET.SubElement(envelope, 'body')
    doh_div = ET.SubElement(body, 'Doh_Div')

    # Header
    ET.SubElement(doh_div, 'Obdobje').text = str(TAX_YEAR)

    # Filter Dividends
    divs = [t for t in transactions if t['Type'] == 'DIV']
    divs.sort(key=lambda x: x['Date'])

    for div in divs:
        item = ET.SubElement(doh_div, 'Doh_Div_Item')
        ET.SubElement(item, 'DatumIzplacila').text = div['Date']

        # Identification
        isin = div['ISIN'] if div['ISIN'] else "UNKNOWN"
        ET.SubElement(item, 'Isin').text = isin
        ET.SubElement(item, 'Naziv').text = div['Name'] if div['Name'] else div['Ticker']

        # Country Logic (First 2 letters of ISIN)
        country_code = get_country_from_isin(isin)
        ET.SubElement(item, 'Drzava').text = country_code
        ET.SubElement(item, 'DrzavaVir').text = country_code

        # Financials
        ET.SubElement(item, 'ZnesekDividende').text = format_decimal(div['TotalValueEUR'], 2)
        ET.SubElement(item, 'TujDavek').text = format_decimal(div['TaxPaidEUR'], 2)
        ET.SubElement(item, 'Valuta').text = "EUR"  # Input is already converted

    return envelope


def save_xml(element, filename):
    xml_str = minidom.parseString(ET.tostring(element)).toprettyxml(indent="  ")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(xml_str)
    print(f"Generated: {filename}")


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    data = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Basic validation
            if row['Type'] in ['BUY', 'SELL', 'DIV']:
                data.append(row)

    print(f"Loaded {len(data)} transactions.")

    # 1. Generate KDVP (Capital Gains)
    if any(x['Type'] in ['BUY', 'SELL'] for x in data):
        xml_kdvp = generate_kdvp(data)
        save_xml(xml_kdvp, FILE_KDVP)
    else:
        print("No trades found, skipping KDVP generation.")

    # 2. Generate Dividends
    if any(x['Type'] == 'DIV' for x in data):
        xml_div = generate_dividends(data)
        save_xml(xml_div, FILE_DIV)
    else:
        print("No dividends found, skipping Dividend generation.")


if __name__ == "__main__":
    main()