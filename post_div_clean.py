"""
post_div_clean.py

Cleans the Doh-Div.xml file by removing invalid dividend entries that would
cause FURS validation errors:
  1. "Znesek dividend mora biti večja od 0" - Dividend value must be greater than 0
  2. "Država izplačevalca dividend" - Payer country must be filled
  3. "Država vira" - Source country must be filled

Usage:
    python post_div_clean.py [input_file] [output_file]

If no arguments provided, reads from Doh-Div.xml and writes to Doh-Div_clean.xml
"""

import xml.etree.ElementTree as ET
from xml.dom import minidom
import sys
import os

# Namespaces used in Doh-Div.xml
NS_DIV_3 = "http://edavki.durs.si/Documents/Schemas/Doh_Div_3.xsd"
NS_EDP = "http://edavki.durs.si/Documents/Schemas/EDP-Common-1.xsd"


def prettify_xml(element):
    """Returns a pretty-printed XML string for the Element."""
    rough_string = ET.tostring(element, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def is_valid_dividend(div_elem, ns):
    """
    Checks if a Dividend element is valid according to FURS requirements:
    - Value must be > 0
    - PayerCountry must be present and non-empty
    - SourceCountry must be present and non-empty (if other fields are filled)
    
    Returns: (is_valid: bool, reason: str)
    """
    # Check Value > 0
    value_elem = div_elem.find(f"{{{ns}}}Value")
    if value_elem is not None and value_elem.text:
        try:
            value = float(value_elem.text)
            if value <= 0:
                return False, "Value <= 0"
        except ValueError:
            return False, "Invalid Value format"
    else:
        return False, "Value is missing"
    
    # Check PayerCountry is present and not empty
    payer_country_elem = div_elem.find(f"{{{ns}}}PayerCountry")
    if payer_country_elem is None or not payer_country_elem.text or payer_country_elem.text.strip() == "":
        return False, "PayerCountry is missing or empty"
    
    # Check SourceCountry is present and not empty
    source_country_elem = div_elem.find(f"{{{ns}}}SourceCountry")
    if source_country_elem is None or not source_country_elem.text or source_country_elem.text.strip() == "":
        return False, "SourceCountry is missing or empty"
    
    return True, "OK"


def clean_dividend_xml(input_file, output_file):
    """
    Reads the dividend XML file, removes invalid entries, and writes the cleaned file.
    """
    # Register namespaces to preserve them in output
    ET.register_namespace('', NS_DIV_3)
    ET.register_namespace('edp', NS_EDP)
    
    # Parse the XML file
    try:
        tree = ET.parse(input_file)
    except ET.ParseError as e:
        print(f"Error parsing XML file: {e}")
        return False
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        return False
    
    root = tree.getroot()
    
    # Find the body element
    body = root.find(f"{{{NS_DIV_3}}}body")
    if body is None:
        # Try without namespace (fallback)
        body = root.find("body")
    
    if body is None:
        print("Error: Could not find <body> element in XML.")
        return False
    
    # Find all Dividend elements
    dividends = body.findall(f"{{{NS_DIV_3}}}Dividend")
    
    removed_count = 0
    removed_entries = []
    
    for div in dividends:
        is_valid, reason = is_valid_dividend(div, NS_DIV_3)
        
        if not is_valid:
            # Get dividend info for reporting
            date_elem = div.find(f"{{{NS_DIV_3}}}Date")
            name_elem = div.find(f"{{{NS_DIV_3}}}PayerName")
            value_elem = div.find(f"{{{NS_DIV_3}}}Value")
            
            date = date_elem.text if date_elem is not None else "N/A"
            name = name_elem.text if name_elem is not None else "N/A"
            value = value_elem.text if value_elem is not None else "N/A"
            
            removed_entries.append({
                'date': date,
                'name': name,
                'value': value,
                'reason': reason
            })
            
            body.remove(div)
            removed_count += 1
    
    # Write the cleaned XML
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(prettify_xml(root))
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Dividend XML Cleaning Report")
    print(f"{'='*60}")
    print(f"Input file:  {input_file}")
    print(f"Output file: {output_file}")
    print(f"Total dividends processed: {len(dividends)}")
    print(f"Invalid entries removed:   {removed_count}")
    print(f"Valid entries remaining:   {len(dividends) - removed_count}")
    
    if removed_entries:
        print(f"\n{'='*60}")
        print("Removed entries:")
        print(f"{'='*60}")
        for i, entry in enumerate(removed_entries, 1):
            print(f"{i:3}. Date: {entry['date']}, Name: {entry['name']}, "
                  f"Value: {entry['value']}, Reason: {entry['reason']}")
    
    print(f"\n[SUCCESS] Cleaned XML written to: {output_file}")
    return True


def main():
    # Default file paths
    default_input = 'Doh-Div.xml'
    default_output = 'Doh-Div_clean.xml'
    
    # Parse command line arguments
    if len(sys.argv) >= 3:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
    elif len(sys.argv) == 2:
        input_file = sys.argv[1]
        output_file = default_output
    else:
        input_file = default_input
        output_file = default_output
    
    # Ensure we're working relative to the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    if not os.path.isabs(input_file):
        input_file = os.path.join(script_dir, input_file)
    if not os.path.isabs(output_file):
        output_file = os.path.join(script_dir, output_file)
    
    # Run the cleaning
    success = clean_dividend_xml(input_file, output_file)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
