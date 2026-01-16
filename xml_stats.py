import xml.etree.ElementTree as ET
import os

FILES = {
    'DIV': 'Doh-Div.xml',
    'OBR': 'Doh-Obr.xml',
    'KDVP': 'Doh-KDVP.xml'
}

def get_text_safe(element, tag_name):
    # Helper to find tag ignoring namespace
    # This is rough but effective for reporting
    for child in element.iter():
        if child.tag.endswith(f"}}{tag_name}") or child.tag == tag_name:
            if child.text:
                return child.text
    return "0.0"

def strip_namespace(tag):
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag

def parse_xml_sums(filepath):
    if not os.path.exists(filepath):
        print(f"[MISSING] {filepath}")
        return None

    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except ET.ParseError:
        print(f"[ERROR] Could not parse {filepath}")
        return None
    
    total = 0.0
    count = 0
    
    # Logic per file type based on known structure
    filename = os.path.basename(filepath)
    
    if 'Doh-Div' in filename:
        # Look for <Value> (Schema v3) or <ZnesekDividende> (Schema v2)
        # We iterate all elements to be schema-agnostic
        for elem in root.iter():
            tag = strip_namespace(elem.tag)
            if tag in ['Value', 'ZnesekDividende']:
                try:
                    val = float(elem.text)
                    total += val
                    count += 1
                except (ValueError, TypeError):
                    pass
                    
    elif 'Doh-Obr' in filename:
        # Look for <Znesek> inside ObrestiItem
        for elem in root.iter():
            tag = strip_namespace(elem.tag)
            if tag == 'Znesek':
                 try:
                    val = float(elem.text)
                    total += val
                    count += 1
                 except:
                    pass

    elif 'Doh-KDVP' in filename:
        # Capital Gains.
        # Report Sales Proceeds (VrednostObOdsvojitvi)
        # And maybe Acquisition Cost (NabavnaVrednost) 
        # But for 'Wealth' usually current value or sales proceeds is meant. 
        # Since this is a tax report for SOLD items, it's "Realized Proceeds".
        
        # NOTE: KDVP also lists acquisitions (NabavnaVrednost) separately in Pridobitev.
        # We only want Odsvojitev -> VrednostObOdsvojitvi to represent "Output" money.
        
        for elem in root.iter():
            tag = strip_namespace(elem.tag)
            if tag == 'VrednostObOdsvojitvi':
                 try:
                    val = float(elem.text)
                    total += val
                    count += 1
                 except:
                    pass

    return total, count

def main():
    print("--- XML Wealth Report ---\n")
    
    grand_total = 0.0
    
    for key, fname in FILES.items():
        if os.path.exists(fname):
            result = parse_xml_sums(fname)
            if result:
                total, count = result
                grand_total += total
                print(f"File: {fname}")
                print(f"  - Count of items: {count}")
                print(f"  - Total Value:    {total:10.2f} EUR")
                if key == 'KDVP':
                    print("    (Note: KDVP sum is 'VrednostObOdsvojitvi' i.e., Sales Proceeds)")
                print("")
        else:
            print(f"File: {fname} NOT FOUND\n")
            
    print("-" * 30)
    print(f"GRAND TOTAL (Proceeds/Income): {grand_total:10.2f} EUR")
    print("-" * 30)

if __name__ == "__main__":
    main()
