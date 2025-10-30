"""
ddi_to_datacite.py

Convert a DDI 2.5 metadata record (XML) to a DataCite 4.x metadata record (XML).
"""

from lxml import etree as ET

# XML namespaces
DDI_NS = {"ddi": "ddi:codebook:2_5"}
OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DATACITE_NS = "http://datacite.org/schema/kernel-4"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XML_NS = "http://www.w3.org/XML/1998/namespace"
XML_LANG = f"{{{XML_NS}}}lang"

# Mandatory fields for DataCite schema
MANDATORY_FIELDS = ["identifier", "creators", "titles", "publisher", "publicationYear", "resourceType"]


def ddi25_to_datacite_xml(ddi_xml: str) -> str:
    """
    Transform a DDI 2.5 XML into a DataCite XML
    wrapped in an OAI-PMH <record> element.

    :param ddi_xml: Raw DDI 2.5 XML string
    :return: Transformed DataCite XML string
    """
    parser = ET.XMLParser(remove_blank_text=True)
    root = ET.fromstring(ddi_xml.encode("utf-8"), parser=parser)

    # Namespaces for output
    NSMAP = {
        None: DATACITE_NS,
        "xsi": XSI_NS
    }

    # --- Create DataCite resource element ---
    resource = ET.Element("resource", nsmap=NSMAP)
    resource.attrib[f"{{{XSI_NS}}}schemaLocation"] = (
        f"{DATACITE_NS} https://schema.datacite.org/meta/kernel-4.6/metadata.xsd"
    )

    # ---- IDENTIFIER ----
    id = root.xpath("string(//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:IDNo/text())", namespaces=DDI_NS)
    if id:
        id_elem = ET.SubElement(resource, "identifier", identifierType="DOI")
        id_elem.text = id.strip()

    # ---- CREATOR(S) ----
    creators_elem = ET.SubElement(resource, "creators")
    creators = root.xpath("//ddi:stdyDscr/ddi:citation/ddi:rspStmt/ddi:AuthEnty", namespaces=DDI_NS)
    if creators:
        for c in creators:
            creator_elem = ET.SubElement(creators_elem, "creator")
            name_elem = ET.SubElement(creator_elem, "creatorName")
            name_elem.text = c.text.strip()
    else:
        print("No creators found in DDI.")

    # ---- TITLES ----
    titles_elem = ET.SubElement(resource, "titles")
    titles = root.xpath("//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:titl", namespaces=DDI_NS)
    for t in titles:
        title_elem = ET.SubElement(titles_elem, "title")
        title_elem.text = t.text.strip()

    # ---- PUBLISHER ----
    publisher_text = root.xpath("string(//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distrbtr)", namespaces=DDI_NS)
    publisher_elem = ET.SubElement(resource, "publisher")
    publisher_elem.text = publisher_text.strip() if publisher_text else "Unknown publisher"

    # ---- PUBLICATION YEAR ----
    pub_year = root.xpath("string(//ddi:stdyDscr/ddi:citation/ddi:verStmt/ddi:version/@date)", namespaces=DDI_NS)
    if not pub_year:
        pub_year = root.xpath("string(//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:prodDate/text())", namespaces=DDI_NS)
    pub_year_elem = ET.SubElement(resource, "publicationYear")
    pub_year_elem.text = pub_year[:4] if pub_year else "0000"

    # ---- RESOURCE TYPE ----
    res_type_elem = ET.SubElement(resource, "resourceType", resourceTypeGeneral="Dataset")
    res_type_elem.text = "Dataset"

    # ---- CONTRIBUTORS ----
    contributors = root.xpath("//ddi:stdyDscr/ddi:citation/ddi:rspStmt/ddi:othId", namespaces=DDI_NS)
    if contributors:
        contribs_elem = ET.SubElement(resource, "contributors")
        for c in contributors:
            contrib_elem = ET.SubElement(contribs_elem, "contributor", contributorType="Other")
            name_elem = ET.SubElement(contrib_elem, "contributorName")
            name_elem.text = c.text.strip()

    # ---- DESCRIPTIONS ----
    abstracts = root.xpath("//ddi:stdyDscr/ddi:stdyInfo/ddi:abstract", namespaces=DDI_NS)
    if abstracts:
        descs_elem = ET.SubElement(resource, "descriptions")
        for a in abstracts:
            desc_elem = ET.SubElement(descs_elem, "description", descriptionType="Abstract")
            desc_elem.text = a.text.strip()

    # ---- SUBJECTS ----
    keywords = root.xpath("//ddi:stdyDscr/ddi:stdyInfo/ddi:subject/ddi:keyword", namespaces=DDI_NS)
    if keywords:
        subjects_elem = ET.SubElement(resource, "subjects")
        for k in keywords:
            subj_elem = ET.SubElement(subjects_elem, "subject")
            subj_elem.text = k.text.strip()

    # ---- LANGUAGES ----
    lang = root.xpath("string(//ddi:stdyDscr/@xml:lang)", namespaces=DDI_NS)
    if lang:
        lang_elem = ET.SubElement(resource, "language")
        lang_elem.text = lang

    # ---- OAI WRAPPER (optional, for consistency with harvested format) ----
    record = ET.Element(f"{{{OAI_NS}}}record", nsmap={"oai": OAI_NS})
    metadata = ET.SubElement(record, f"{{{OAI_NS}}}metadata")
    metadata.append(resource)

    # --- Return as pretty string ---
    return ET.tostring(
        record,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8"
    ).decode("utf-8")


# CLI for testing
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python ddi_to_datacite.py input.xml output.xml")
        sys.exit(1)

    with open(sys.argv[1], "r", encoding="utf-8") as f:
        ddi_xml = f.read()

    datacite_xml = ddi25_to_datacite_xml(ddi_xml)

    with open(sys.argv[2], "w", encoding="utf-8") as f:
        f.write(datacite_xml)

    print(f"Converted {sys.argv[1]} → {sys.argv[2]}")
