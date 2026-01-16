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
MY_TAX_ID = "12345678"  # <--- REPLACE WITH YOUR REAL TAX ID
# ---------------------

NS_EDP = "http://edavki.durs.si/Documents/Schemas/EDP-Common-1.xsd"
NS_KDVP = "http://edavki.durs.si/Documents/Schemas/Doh_KDVP_9.xsd"
NS_DIV = "http://edavki.durs.si/Documents/Schemas/Doh_Div_2.xsd"
NS_OBR = "http://edavki.durs.si/Documents/Schemas/Doh_Obr_2.xsd"


def create_header(parent):
    header = ET.SubElement(parent, f"{{{NS_EDP}}}Header")
    taxpayer = ET.SubElement(header, f"{{{NS_EDP}}}taxpayer")
    ET.SubElement(taxpayer, f"{{{NS_EDP}}}taxNumber").text = str(MY_TAX_ID)
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


# --- KDVP (Stocks) ---
def generate_kdvp(transactions):
    ET.register_namespace('', NS_KDVP)
    ET.register_namespace('edp', NS_EDP)
    envelope = ET.Element(f"{{{NS_KDVP}}}Envelope")
    create_header(envelope)

    body = ET.SubElement(envelope, f"{{{NS_KDVP}}}body")
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

    for ticker, trades in grouped.items():
        if not any(t['Type'] == 'SELL' and t['Date'].startswith(str(TAX_YEAR)) for t in trades):
            continue

        kdvp_item = ET.SubElement(kdvp, f"{{{NS_KDVP}}}KDVPItem")
        popisni_list = ET.SubElement(kdvp_item, f"{{{NS_KDVP}}}PopisniList")
        ET.SubElement(popisni_list, f"{{{NS_KDVP}}}Naziv").text = trades[0]['Name']
        ET.SubElement(popisni_list, f"{{{NS_KDVP}}}Isin").text = trades[0]['ISIN'] or ticker

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
    return envelope


# --- INTEREST (Doh-Obr) ---
def generate_interest(transactions):
    ET.register_namespace('', NS_OBR)
    ET.register_namespace('edp', NS_EDP)
    envelope = ET.Element(f"{{{NS_OBR}}}Envelope")
    create_header(envelope)

    body = ET.SubElement(envelope, f"{{{NS_OBR}}}body")
    doh_obr = ET.SubElement(body, f"{{{NS_OBR}}}Doh_Obr")
    ET.SubElement(doh_obr, f"{{{NS_OBR}}}Obdobje").text = str(TAX_YEAR)

    obresti = ET.SubElement(doh_obr, f"{{{NS_OBR}}}Obresti")

    # Filter for Interest types
    int_items = [t for t in transactions if
                 t['Type'] in ['INTEREST', 'LENDING'] and t['Date'].startswith(str(TAX_YEAR))]

    for t in int_items:
        # LOGIC:
        # Revolut -> Code 1 (Bank)
        # T212 -> Code 3 (Broker/Other)
        # IBKR -> Code 3

        code = '3'  # Default to fully taxed
        country = 'GB'  # Default

        if t['Source'] == 'Revolut':
            code = '1'
            country = 'LT'  # Lithuania
        elif t['Source'] == 'T212':
            code = '3'
            country = 'GB'  # Or CY (Cyprus) depending on entity, GB is safer default for T212

        row = ET.SubElement(obresti, f"{{{NS_OBR}}}ObrestiItem")
        ET.SubElement(row, f"{{{NS_OBR}}}DatumPrejetja").text = t['Date']
        ET.SubElement(row, f"{{{NS_OBR}}}VrstaObresti").text = code
        ET.SubElement(row, f"{{{NS_OBR}}}Opis").text = t['Name']  # e.g. "T212 Interest"
        ET.SubElement(row, f"{{{NS_OBR}}}Znesek").text = format_decimal(t['TotalValueEUR'], 2)
        ET.SubElement(row, f"{{{NS_OBR}}}Drzava").text = country
        ET.SubElement(row, f"{{{NS_OBR}}}TujDavek").text = format_decimal(t['TaxPaidEUR'], 2)

        if float(t['TaxPaidEUR']) > 0:
            ET.SubElement(row, f"{{{NS_OBR}}}DrzavaVir").text = country

    return envelope


# --- MAIN ---
def main():
    if not os.path.exists(INPUT_FILE): return
    data = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader: data.append(row)

    # 1. KDVP
    with open(FILE_KDVP, 'w', encoding='utf-8') as f:
        f.write(prettify_xml(generate_kdvp(data)))

    # 2. DIVIDENDS (using previous logic, just calling it here)
    # (Copy the generate_dividends function from previous script into here if needed)

    # 3. INTEREST
    with open(FILE_OBR, 'w', encoding='utf-8') as f:
        f.write(prettify_xml(generate_interest(data)))

    print(f"\n[SUCCESS] Generated {FILE_KDVP} (Stocks) and {FILE_OBR} (Interest/Lending).")


if __name__ == "__main__":
    main()