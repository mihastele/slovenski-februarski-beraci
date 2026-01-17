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
    # Copy children to the namespaced header
    # valid_header_tags = {'taxNumber', 'taxpayerType', 'name', 'address1', 'city', 'postNumber', 'postName', 'resident', 'countryID'}
    # Actually based on user sample, 'resident' and 'countryID' were NOT in header. 
    # User's header tags: taxNumber, taxpayerType, name, address1, city, postNumber, postName.
    # We will be conservative and include only what's seen in valid EDAVKI headers usually.
    # The common schema usually allows resident/countryID but maybe strictly for some forms?
    # Let's stick to the sample provided:
    allowed_tags = ['taxNumber', 'taxpayerType', 'name', 'address1', 'city', 'postNumber', 'postName']
    
    for child in user_taxpayer:
        if child.tag in allowed_tags:
            new_child = ET.SubElement(taxpayer_node, f"{{{NS_EDP}}}{child.tag}")
            new_child.text = child.text

    workflow = ET.SubElement(header, f"{{{NS_EDP}}}Workflow")
    ET.SubElement(workflow, f"{{{NS_EDP}}}DocumentWorkflowID").text = "O"
    return header


# def format_decimal(value, precision=2):
#     try:
#         return f"{float(value):.{precision}f}"
#     except:
#         return f"{0:.{precision}f}"


def format_decimal(value, precision=2, require_nonzero=False):
    """
    Returns a formatted decimal string or None.
    If require_nonzero=True, returns None when the value would format to 0 at given precision.
    """
    try:
        v = float(value)
    except:
        v = 0.0

    rounded = round(v, precision)

    if require_nonzero and rounded == 0:
        return None

    return f"{rounded:.{precision}f}"


def add_decimal_el(parent, tag, value, precision=2, require_nonzero=False, omit_if_zero=False):
    """
    Creates subelement 'tag' with formatted decimal text.
    - If omit_if_zero=True: does nothing when rounded value == 0.
    - If require_nonzero=True: returns False when it would be zero (caller should skip the whole item).
    Returns True if OK, False if caller should skip.
    """
    s = format_decimal(value, precision, require_nonzero=require_nonzero)
    if s is None:
        return False  # caller should skip the whole item

    if omit_if_zero:
        try:
            if float(s) == 0.0:
                return True
        except:
            pass

    ET.SubElement(parent, tag).text = s
    return True



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

    body = ET.SubElement(envelope, f"{{{NS_KDVP}}}body")
    ET.SubElement(body, f"{{{NS_EDP}}}bodyContent") # Required by schema
    doh_kdvp = ET.SubElement(body, f"{{{NS_KDVP}}}Doh_KDVP")
    kdvp = ET.SubElement(doh_kdvp, f"{{{NS_KDVP}}}KDVP")

    doc_header = ET.SubElement(kdvp, f"{{{NS_KDVP}}}DocumentWorkflowID").text = "O" # Using DocumentWorkflowID as placeholder/required if 'O' was for 'Original'? No, 'O' was VrstaPoro. 
    # Checking XSD again for KDVP:
    # <xs:element name="Year" type="typeYear" minOccurs="0">
    # <xs:element name="DocumentWorkflowID" ... minOccurs="0">
    
    # Original code had:
    # <Dokument><Glava><Obdobje>2025</Obdobje><VrstaPoro>O</VrstaPoro></Glava></Dokument>
    # This looks like old schema structure.
    # New KDVP schema 9 has top level elements in KDVP.
    
    ET.SubElement(kdvp, f"{{{NS_KDVP}}}DocumentWorkflowID").text = "O" 
    ET.SubElement(kdvp, f"{{{NS_KDVP}}}Year").text = str(TAX_YEAR)

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

            qty_ok = add_decimal_el(row, f"{{{NS_KDVP}}}Kolicina", t['Quantity'], precision=4)
            val_ok = add_decimal_el(row, f"{{{NS_KDVP}}}NabavnaVrednost", t['TotalValueEUR'], precision=4)

            if not (qty_ok and val_ok):
                # Skip this BUY row entirely (or log it)
                pridobitve.remove(row)  # or restructure to only create 'row' after checks
                continue

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
    NS_DIV_3 = "http://edavki.durs.si/Documents/Schemas/Doh_Div_3.xsd"
    ET.register_namespace('', NS_DIV_3)
    ET.register_namespace('edp', NS_EDP)
    
    envelope = ET.Element(f"{{{NS_DIV_3}}}Envelope")
    create_header(envelope)
    ET.SubElement(envelope, f"{{{NS_EDP}}}Signatures")

    # Body has no namespace prefix in the sample, implies default namespace (NS_DIV_3)
    body = ET.SubElement(envelope, f"{{{NS_DIV_3}}}body")
    ET.SubElement(body, f"{{{NS_EDP}}}bodyContent") # Required by schema
    
    # Metadata part
    doh_div = ET.SubElement(body, f"{{{NS_DIV_3}}}Doh_Div")
    ET.SubElement(doh_div, f"{{{NS_DIV_3}}}Period").text = str(TAX_YEAR)
    
    # Load taxpayer extradata for the body part
    user_taxpayer = load_taxpayer_data()
    # Map from taxpayer.xml tags to Doh_Div tags
    # taxpayer.xml: email -> EmailAddress
    # taxpayer.xml: telephoneNumber -> PhoneNumber
    # taxpayer.xml: residentCountry -> ResidentCountry
    # taxpayer.xml: isResident -> IsResident
    
    mapping = {
        'email': 'EmailAddress',
        'telephoneNumber': 'PhoneNumber',
        'residentCountry': 'ResidentCountry',
        'isResident': 'IsResident'
    }
    
    for child in user_taxpayer:
        if child.tag in mapping:
            ET.SubElement(doh_div, f"{{{NS_DIV_3}}}{mapping[child.tag]}").text = child.text

    divs = [t for t in transactions if t['Type'] == 'DIV' and t['Date'].startswith(str(TAX_YEAR))]
    divs.sort(key=lambda x: x['Date'])

    for t in divs:
        # Payer info from transaction or default if missing? 
        # The sample has PayerIdentificationNumber, Name, Address, Country.
        # Our CSV data might not have all this. We'll do best effort or placeholders.

        # VALIDATION 1: Value > 0
        value_str = format_decimal(t['TotalValueEUR'], 2)
        if value_str is None or float(value_str) <= 0:
            # Too small -> would become 0.00, skip dividend item. Or explicit 0.
            continue
        
        div_node = ET.SubElement(doh_div, f"{{{NS_DIV_3}}}Dividend") # NOTE: Parent is Doh_Div, not body directly!
        ET.SubElement(div_node, f"{{{NS_DIV_3}}}Date").text = t['Date']
        
        isin = t['ISIN'] if t['ISIN'] else "UNKNOWN"
        # Extract country code from ISIN (first 2 chars) or default to US
        country_code = isin[:2].upper() if len(isin) >= 2 and isin[:2].isalpha() else "US"
        
        # VALIDATION 2: PayerCountry must be filled
        payer_country = country_code # Simplification: Assume Payer Country = ISIN prefix
        
        # VALIDATION 3: SourceCountry must be filled if filled (mutually required generally)
        source_country = payer_country

        ET.SubElement(div_node, f"{{{NS_DIV_3}}}PayerIdentificationNumber").text = "00000000" # Placeholder
        ET.SubElement(div_node, f"{{{NS_DIV_3}}}PayerName").text = t['Name'] if t['Name'] else "Unknown"
        ET.SubElement(div_node, f"{{{NS_DIV_3}}}PayerAddress").text = "Unknown Address"
        ET.SubElement(div_node, f"{{{NS_DIV_3}}}PayerCountry").text = payer_country
        ET.SubElement(div_node, f"{{{NS_DIV_3}}}Type").text = "1" # 1 = Dividende
        ET.SubElement(div_node, f"{{{NS_DIV_3}}}Value").text = value_str

        tax_str = format_decimal(t['TaxPaidEUR'], 2)
        
        # FURS Validation logic:
        # If one field is filled, others might be required.
        # Typically for foreign dividends: ForeignTax and SourceCountry are key.
        
        # Always fill ForeignTax (0.00 if empty) and SourceCountry for robustness
        ET.SubElement(div_node, f"{{{NS_DIV_3}}}ForeignTax").text = tax_str if tax_str else "0.00"
        ET.SubElement(div_node, f"{{{NS_DIV_3}}}SourceCountry").text = source_country
        
        # ReliefStatement is mandatory if tax treaty claimed? Sample has it.
        # We'll leave it empty or omit if not claiming treaty in this simplified script.
        # Sample: <ReliefStatement>10/01, 2b odstavek 10. člena</ReliefStatement>
        # ET.SubElement(div_node, f"{{{NS_DIV_3}}}ReliefStatement").text = "" 

    return envelope


# --- 3. INTEREST (Obr) ---
def generate_obr(transactions):
    ET.register_namespace('', NS_OBR)
    ET.register_namespace('edp', NS_EDP)
    envelope = ET.Element(f"{{{NS_OBR}}}Envelope")
    create_header(envelope)
    ET.SubElement(envelope, f"{{{NS_EDP}}}Signatures")

    body = ET.SubElement(envelope, f"{{{NS_OBR}}}body")
    ET.SubElement(body, f"{{{NS_EDP}}}bodyContent") # Required
    doh_obr = ET.SubElement(body, f"{{{NS_OBR}}}Doh_Obr")
    ET.SubElement(doh_obr, f"{{{NS_OBR}}}Period").text = str(TAX_YEAR)
    
    # Add required metadata elements in correct order before Obresti
    ET.SubElement(doh_obr, f"{{{NS_OBR}}}DocumentWorkflowID").text = "O"
    
    # Load taxpayer data for metadata fields
    user_taxpayer = load_taxpayer_data()
    
    # Create a dictionary for easy lookup
    taxpayer_data = {child.tag: child.text for child in user_taxpayer}
    
    # Add elements in the exact order required by XSD schema
    if 'email' in taxpayer_data:
        ET.SubElement(doh_obr, f"{{{NS_OBR}}}Email").text = taxpayer_data['email']
    if 'telephoneNumber' in taxpayer_data:
        ET.SubElement(doh_obr, f"{{{NS_OBR}}}TelephoneNumber").text = taxpayer_data['telephoneNumber']
    if 'isResident' in taxpayer_data:
        ET.SubElement(doh_obr, f"{{{NS_OBR}}}ResidentOfRepublicOfSlovenia").text = taxpayer_data['isResident']
    if 'residentCountry' in taxpayer_data:
        ET.SubElement(doh_obr, f"{{{NS_OBR}}}Country").text = taxpayer_data['residentCountry']

    obresti = ET.SubElement(doh_obr, f"{{{NS_OBR}}}Interest")

    items = [t for t in transactions if t['Type'] in ['INTEREST', 'LENDING'] and t['Date'].startswith(str(TAX_YEAR))]

    for t in items:
        znesek_str = format_decimal(t['TotalValueEUR'], 2)
        if znesek_str is None:
            continue  # skip interest item that would be 0.00
        # Default Logic:
        # Revolut = Code 1 (Bank, 1000eur limit)
        # T212/IBKR = Code 3 (Other/Broker, Full tax)
        code = '1' if t['Source'] == 'Revolut' else '3'
        country = 'LT' if t['Source'] == 'Revolut' else 'GB'
        tuj_str = format_decimal(t['TaxPaidEUR'], 2)

        row = ET.SubElement(obresti, f"{{{NS_OBR}}}ObrestiItem")
        ET.SubElement(row, f"{{{NS_OBR}}}DatumPrejetja").text = t['Date']
        ET.SubElement(row, f"{{{NS_OBR}}}VrstaObresti").text = code
        ET.SubElement(row, f"{{{NS_OBR}}}Opis").text = f"{t['Source']} Interest"
        ET.SubElement(row, f"{{{NS_OBR}}}Znesek").text = format_decimal(t['TotalValueEUR'], 2)
        ET.SubElement(row, f"{{{NS_OBR}}}Drzava").text = country
        ET.SubElement(row, f"{{{NS_OBR}}}TujDavek").text = format_decimal(t['TaxPaidEUR'], 2)
        if tuj_str is not None and float(tuj_str) != 0.0:
            ET.SubElement(row, f"{{{NS_OBR}}}TujDavek").text = tuj_str
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