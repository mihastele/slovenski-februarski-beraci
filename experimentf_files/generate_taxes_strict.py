import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom
from collections import defaultdict
import os

# --- CONFIGURATION ---
INPUT_FILE = '../master_data.csv'
FILE_KDVP = '../Doh-KDVP.xml'
FILE_DIV = '../Doh-Div.xml'
FILE_OBR = '../Doh-Obr.xml'
TAX_YEAR = 2025

# !!! CRITICAL FOR VALID XML !!!
# eDavki requires a Tax Number in the header to validate against the schema.
MY_TAX_ID = "12345678"  # <--- REPLACE THIS WITH YOUR REAL TAX ID
# ---------------------

# Namespaces
NS_EDP = "http://edavki.durs.si/Documents/Schemas/EDP-Common-1.xsd"
NS_KDVP = "http://edavki.durs.si/Documents/Schemas/Doh_KDVP_9.xsd"
NS_DIV = "http://edavki.durs.si/Documents/Schemas/Doh_Div_2.xsd"
NS_OBR = "http://edavki.durs.si/Documents/Schemas/Doh_Obr_2.xsd"


def create_header(parent):
    """Creates the standard edp:Header required by EDP-Common-1.xsd"""
    # Note: We use QName with registered namespace to force 'edp:' prefix
    header = ET.SubElement(parent, f"{{{NS_EDP}}}Header")

    taxpayer = ET.SubElement(header, f"{{{NS_EDP}}}taxpayer")
    # The schema requires taxNumber.
    ET.SubElement(taxpayer, f"{{{NS_EDP}}}taxNumber").text = str(MY_TAX_ID)

    # Workflow is required by schema, even if empty or basic
    ET.SubElement(header, f"{{{NS_EDP}}}Workflow")

    return header


def format_decimal(value, precision=4):
    try:
        return f"{float(value):.{precision}f}"
    except:
        return f"{0:.{precision}f}"


def prettify_xml(element):
    """Returns a pretty-printed XML string with correct namespace prefixes"""
    rough_string = ET.tostring(element, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


# --- 1. CAPITAL GAINS (Doh-KDVP) ---
def generate_kdvp(transactions):
    # Register Namespaces for this document
    ET.register_namespace('', NS_KDVP)
    ET.register_namespace('edp', NS_EDP)

    envelope = ET.Element(f"{{{NS_KDVP}}}Envelope")
    create_header(envelope)

    body = ET.SubElement(envelope, f"{{{NS_KDVP}}}body")
    doh_kdvp = ET.SubElement(body, f"{{{NS_KDVP}}}Doh_KDVP")
    kdvp = ET.SubElement(doh_kdvp, f"{{{NS_KDVP}}}KDVP")

    # Document Metadata
    doc_header = ET.SubElement(kdvp, f"{{{NS_KDVP}}}Dokument")
    glava = ET.SubElement(doc_header, f"{{{NS_KDVP}}}Glava")
    ET.SubElement(glava, f"{{{NS_KDVP}}}Obdobje").text = str(TAX_YEAR)
    ET.SubElement(glava, f"{{{NS_KDVP}}}VrstaPoro").text = 'O'

    # Filter: Only items with a SELL in the tax year
    grouped = defaultdict(list)
    for t in transactions:
        if t['Type'] in ['BUY', 'SELL']:
            grouped[t['Ticker']].append(t)

    count = 0
    for ticker, trades in grouped.items():
        # Smart Filter: Check for realized gains in TAX_YEAR
        if not any(t['Type'] == 'SELL' and t['Date'].startswith(str(TAX_YEAR)) for t in trades):
            continue

        count += 1
        isin = next((t['ISIN'] for t in trades if t['ISIN']), ticker)
        name = next((t['Name'] for t in trades if t['Name']), ticker)

        kdvp_item = ET.SubElement(kdvp, f"{{{NS_KDVP}}}KDVPItem")
        popisni_list = ET.SubElement(kdvp_item, f"{{{NS_KDVP}}}PopisniList")
        ET.SubElement(popisni_list, f"{{{NS_KDVP}}}Naziv").text = name
        ET.SubElement(popisni_list, f"{{{NS_KDVP}}}Isin").text = isin

        pridobitve = ET.SubElement(popisni_list, f"{{{NS_KDVP}}}Pridobitve")
        odsvojitve = ET.SubElement(popisni_list, f"{{{NS_KDVP}}}Odsvojitve")

        trades.sort(key=lambda x: x['Date'])

        for t in trades:
            if t['Type'] == 'BUY':
                row = ET.SubElement(pridobitve, f"{{{NS_KDVP}}}Pridobitev")
                ET.SubElement(row, f"{{{NS_KDVP}}}DatumPridobitve").text = t['Date']
                ET.SubElement(row, f"{{{NS_KDVP}}}Kolicina").text = format_decimal(t['Quantity'], 4)
                ET.SubElement(row, f"{{{NS_KDVP}}}NabavnaVrednost").text = format_decimal(t['TotalValueEUR'], 4)
                ET.SubElement(row, f"{{{NS_KDVP}}}NacinPridobitve").text = "A"
            elif t['Type'] == 'SELL':
                row = ET.SubElement(odsvojitve, f"{{{NS_KDVP}}}Odsvojitev")
                ET.SubElement(row, f"{{{NS_KDVP}}}DatumOdsvojitve").text = t['Date']
                ET.SubElement(row, f"{{{NS_KDVP}}}Kolicina").text = format_decimal(t['Quantity'], 4)
                ET.SubElement(row, f"{{{NS_KDVP}}}VrednostObOdsvojitvi").text = format_decimal(t['TotalValueEUR'], 4)
                ET.SubElement(row, f"{{{NS_KDVP}}}NacinOdsvojitve").text = "A"

    print(f"KDVP: Generated for {count} tickers.")
    return envelope


# --- 2. DIVIDENDS (Doh-Div) ---
def generate_dividends(transactions):
    ET.register_namespace('', NS_DIV)
    ET.register_namespace('edp', NS_EDP)

    envelope = ET.Element(f"{{{NS_DIV}}}Envelope")
    create_header(envelope)

    body = ET.SubElement(envelope, f"{{{NS_DIV}}}body")
    doh_div = ET.SubElement(body, f"{{{NS_DIV}}}Doh_Div")
    ET.SubElement(doh_div, f"{{{NS_DIV}}}Obdobje").text = str(TAX_YEAR)

    divs = [t for t in transactions if t['Type'] == 'DIV' and t['Date'].startswith(str(TAX_YEAR))]
    divs.sort(key=lambda x: x['Date'])

    for t in divs:
        item = ET.SubElement(doh_div, f"{{{NS_DIV}}}Doh_Div_Item")
        ET.SubElement(item, f"{{{NS_DIV}}}DatumIzplacila").text = t['Date']

        isin = t['ISIN'] if t['ISIN'] else "UNKNOWN"
        country = isin[:2].upper() if len(isin) >= 2 else "US"

        ET.SubElement(item, f"{{{NS_DIV}}}Isin").text = isin
        ET.SubElement(item, f"{{{NS_DIV}}}Naziv").text = t['Name'] or t['Ticker']
        ET.SubElement(item, f"{{{NS_DIV}}}Drzava").text = country
        ET.SubElement(item, f"{{{NS_DIV}}}DrzavaVir").text = country
        ET.SubElement(item, f"{{{NS_DIV}}}ZnesekDividende").text = format_decimal(t['TotalValueEUR'], 2)
        ET.SubElement(item, f"{{{NS_DIV}}}TujDavek").text = format_decimal(t['TaxPaidEUR'], 2)
        ET.SubElement(item, f"{{{NS_DIV}}}Valuta").text = "EUR"

    print(f"Dividends: Generated {len(divs)} entries.")
    return envelope


# --- 3. INTEREST (Doh-Obr) ---
# NOTE: This requires the interest data detection from the previous answer.
# For this script, I will scan master_data.csv for a "virtual" type if you added one,
# OR we assume you ran generate_interest.py separately.
# However, to be strict, let's create a placeholder structure here if we had data.
def generate_interest(interest_items):
    ET.register_namespace('', NS_OBR)
    ET.register_namespace('edp', NS_EDP)

    envelope = ET.Element(f"{{{NS_OBR}}}Envelope")
    create_header(envelope)

    body = ET.SubElement(envelope, f"{{{NS_OBR}}}body")
    doh_obr = ET.SubElement(body, f"{{{NS_OBR}}}Doh_Obr")
    ET.SubElement(doh_obr, f"{{{NS_OBR}}}Obdobje").text = str(TAX_YEAR)

    obresti = ET.SubElement(doh_obr, f"{{{NS_OBR}}}Obresti")

    for item in interest_items:
        row = ET.SubElement(obresti, f"{{{NS_OBR}}}ObrestiItem")
        ET.SubElement(row, f"{{{NS_OBR}}}DatumPrejetja").text = item['Date']
        ET.SubElement(row, f"{{{NS_OBR}}}VrstaObresti").text = item['Code']
        ET.SubElement(row, f"{{{NS_OBR}}}Opis").text = item['Description']
        ET.SubElement(row, f"{{{NS_OBR}}}Znesek").text = format_decimal(item['Amount'], 2)
        ET.SubElement(row, f"{{{NS_OBR}}}Drzava").text = item['Country']
        ET.SubElement(row, f"{{{NS_OBR}}}TujDavek").text = format_decimal(item['TaxPaid'], 2)
        if item['TaxPaid'] > 0:
            ET.SubElement(row, f"{{{NS_OBR}}}DrzavaVir").text = item['Country']

    return envelope


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Run 'convert_all_providers.py' first.")
        return

    data = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Ensure numbers are floats
            row['Quantity'] = float(row['Quantity'] or 0)
            row['TotalValueEUR'] = float(row['TotalValueEUR'] or 0)
            row['TaxPaidEUR'] = float(row['TaxPaidEUR'] or 0)
            data.append(row)

    # 1. KDVP
    xml_kdvp = generate_kdvp(data)
    with open(FILE_KDVP, 'w', encoding='utf-8') as f:
        f.write(prettify_xml(xml_kdvp))

    # 2. Dividends
    xml_div = generate_dividends(data)
    with open(FILE_DIV, 'w', encoding='utf-8') as f:
        f.write(prettify_xml(xml_div))

    # 3. Interest (If you have a separate list from the previous Interest script)
    # You can integrate the interest logic here if you wish.
    # For now, this script ensures KDVP and DIV are strict.

    print("\n[SUCCESS] Strict XMLs generated.")
    print(f"1. {FILE_KDVP} -> Import via 'Uvoz popisnih listov' (Import Inventory Sheets)")
    print(f"2. {FILE_DIV}  -> Import via 'Uvoz dokumenta' (Import Document)")
    print("\nReminder: Check if '12345678' in the XML matches your Tax ID!")


if __name__ == "__main__":
    main()