import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom
from collections import defaultdict
import os

# --- CONFIGURATION ---
INPUT_FILE = 'master_data.csv'
FILE_KDVP = 'Doh-KDVP.xml'
FILE_DIV = 'Doh-Div.xml'
FILE_OBR = 'Doh-Obr.xml'
TAX_YEAR = 2025
# ---------------------

NS_EDP = "http://edavki.durs.si/Documents/Schemas/EDP-Common-1.xsd"
NS_KDVP = "http://edavki.durs.si/Documents/Schemas/Doh_KDVP_9.xsd"
NS_DIV = "http://edavki.durs.si/Documents/Schemas/Doh_Div_2.xsd"
NS_OBR = "http://edavki.durs.si/Documents/Schemas/Doh_Obr_2.xsd"


def load_taxpayer_data():
    xml_file = os.path.join(os.path.dirname(__file__), 'taxpayer.xml')
    if not os.path.exists(xml_file):
        raise FileNotFoundError("Config file 'taxpayer.xml' not found. Please copy 'taxpayer.example.xml' to 'taxpayer.xml' and fill it.")
    try:
        tree = ET.parse(xml_file)
        return tree.getroot()
    except ET.ParseError:
        raise ValueError("Invalid XML in 'taxpayer.xml'.")

def create_header(parent):
    header = ET.SubElement(parent, f"{{{NS_EDP}}}Header")
    taxpayer_node = ET.SubElement(header, f"{{{NS_EDP}}}taxpayer")
    
    # Load data from local XML
    user_taxpayer = load_taxpayer_data()
    
    # Copy children to the namespaced header
    for child in user_taxpayer:
        # We assume tag names in taxpayer.xml match required EDP schema names (e.g. taxNumber, name, ...)
        new_child = ET.SubElement(taxpayer_node, f"{{{NS_EDP}}}{child.tag}")
        new_child.text = child.text

    ET.SubElement(header, f"{{{NS_EDP}}}Workflow")
    return header


def format_decimal(value, precision=2):
    try:
        return f"{float(value):.{precision}f}"
    except:
        return f"{0:.{precision}f}"


def prettify_xml(element):
    rough_string = ET.tostring(element, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


# --- 1. STOCKS (KDVP) ---
def generate_kdvp(transactions):
    ET.register_namespace('', NS_KDVP)
    ET.register_namespace('edp', NS_EDP)
    envelope = ET.Element(f"{{{NS_KDVP}}}Envelope")
    create_header(envelope)
    ET.SubElement(envelope, f"{{{NS_EDP}}}Signatures")

    body = ET.SubElement(envelope, f"{{{NS_EDP}}}body")
    doh_kdvp = ET.SubElement(body, f"{{{NS_KDVP}}}Doh_KDVP")
    kdvp = ET.SubElement(doh_kdvp, f"{{{NS_KDVP}}}KDVP")

    doc_header = ET.SubElement(kdvp, f"{{{NS_KDVP}}}Dokument")
    glava = ET.SubElement(doc_header, f"{{{NS_KDVP}}}Glava")
    ET.SubElement(glava, f"{{{NS_KDVP}}}Obdobje").text = str(TAX_YEAR)
    ET.SubElement(glava, f"{{{NS_KDVP}}}VrstaPoro").text = 'O'

    grouped = defaultdict(list)
    for t in transactions:
        if t['Type'] in ['BUY', 'SELL']:
            grouped[t['Ticker']].append(t)

    count = 0
    for ticker, trades in grouped.items():
        # FILTER: Only report if there is a SELL in the tax year
        if not any(t['Type'] == 'SELL' and t['Date'].startswith(str(TAX_YEAR)) for t in trades):
            continue

        count += 1
        name = next((t['Name'] for t in trades if t['Name']), ticker)
        isin = next((t['ISIN'] for t in trades if t['ISIN']), ticker)

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

    print(f"KDVP: Reporting {count} tickers.")
    return envelope


# --- 2. DIVIDENDS (Div) ---
def generate_div(transactions):
    ET.register_namespace('', NS_DIV)
    ET.register_namespace('edp', NS_EDP)
    envelope = ET.Element(f"{{{NS_DIV}}}Envelope")
    create_header(envelope)
    ET.SubElement(envelope, f"{{{NS_EDP}}}Signatures")

    body = ET.SubElement(envelope, f"{{{NS_EDP}}}body")
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
        ET.SubElement(item, f"{{{NS_DIV}}}Naziv").text = t['Name']
        ET.SubElement(item, f"{{{NS_DIV}}}Drzava").text = country
        ET.SubElement(item, f"{{{NS_DIV}}}DrzavaVir").text = country
        ET.SubElement(item, f"{{{NS_DIV}}}ZnesekDividende").text = format_decimal(t['TotalValueEUR'], 2)
        ET.SubElement(item, f"{{{NS_DIV}}}TujDavek").text = format_decimal(t['TaxPaidEUR'], 2)
        ET.SubElement(item, f"{{{NS_DIV}}}Valuta").text = "EUR"
    return envelope


# --- 3. INTEREST (Obr) ---
def generate_obr(transactions):
    ET.register_namespace('', NS_OBR)
    ET.register_namespace('edp', NS_EDP)
    envelope = ET.Element(f"{{{NS_OBR}}}Envelope")
    create_header(envelope)
    ET.SubElement(envelope, f"{{{NS_EDP}}}Signatures")

    body = ET.SubElement(envelope, f"{{{NS_EDP}}}body")
    doh_obr = ET.SubElement(body, f"{{{NS_OBR}}}Doh_Obr")
    ET.SubElement(doh_obr, f"{{{NS_OBR}}}Obdobje").text = str(TAX_YEAR)

    obresti = ET.SubElement(doh_obr, f"{{{NS_OBR}}}Obresti")

    items = [t for t in transactions if t['Type'] in ['INTEREST', 'LENDING'] and t['Date'].startswith(str(TAX_YEAR))]

    for t in items:
        # Default Logic:
        # Revolut = Code 1 (Bank, 1000eur limit)
        # T212/IBKR = Code 3 (Other/Broker, Full tax)
        code = '1' if t['Source'] == 'Revolut' else '3'
        country = 'LT' if t['Source'] == 'Revolut' else 'GB'

        row = ET.SubElement(obresti, f"{{{NS_OBR}}}ObrestiItem")
        ET.SubElement(row, f"{{{NS_OBR}}}DatumPrejetja").text = t['Date']
        ET.SubElement(row, f"{{{NS_OBR}}}VrstaObresti").text = code
        ET.SubElement(row, f"{{{NS_OBR}}}Opis").text = f"{t['Source']} Interest"
        ET.SubElement(row, f"{{{NS_OBR}}}Znesek").text = format_decimal(t['TotalValueEUR'], 2)
        ET.SubElement(row, f"{{{NS_OBR}}}Drzava").text = country
        ET.SubElement(row, f"{{{NS_OBR}}}TujDavek").text = format_decimal(t['TaxPaidEUR'], 2)
        if float(t['TaxPaidEUR']) > 0:
            ET.SubElement(row, f"{{{NS_OBR}}}DrzavaVir").text = country

    return envelope


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} missing. Run Step 1 script first.")
        return

    data = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader: data.append(row)

    # Generate XMLs
    with open(FILE_KDVP, 'w', encoding='utf-8') as f:
        f.write(prettify_xml(generate_kdvp(data)))
    with open(FILE_DIV, 'w', encoding='utf-8') as f:
        f.write(prettify_xml(generate_div(data)))
    with open(FILE_OBR, 'w', encoding='utf-8') as f:
        f.write(prettify_xml(generate_obr(data)))

    print("\n[SUCCESS] All XMLs generated!")
    print(f"  - {FILE_KDVP} (Stocks -> Inventory Import)")
    print(f"  - {FILE_DIV}  (Dividends -> Document Import)")
    print(f"  - {FILE_OBR}  (Interest -> Import directly into Form)")


if __name__ == "__main__":
    main()