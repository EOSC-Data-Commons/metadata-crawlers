from __future__ import annotations

import xml.etree.ElementTree as ET
from xml.dom import minidom

from .models import HarvestedRecord

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
DATACITE_NS = "http://datacite.org/schema/kernel-4"
DATACITE_SCHEMA_LOCATION = (
    "http://datacite.org/schema/kernel-4 "
    "https://schema.datacite.org/meta/kernel-4.6/metadata.xsd"
)


def _sub(parent: ET.Element, tag: str, text: str | None = None, **attrs: str) -> ET.Element:
    el = ET.SubElement(parent, tag, attrs)
    if text is not None:
        el.text = text
    return el


def add_orcid(parent_el: ET.Element, orcid: str | None) -> None:
    """
    Attach a DataCite <nameIdentifier> element for an ORCID iD, if present.

    :param parent_el: The `creator` or `contributor` element to attach to.
    :param orcid: The ORCID iD, bare or as a full URL, or None.
    :return: None. `parent_el` is mutated in place.
    """
    if not orcid:
        return
    orcid = orcid.strip()
    if not orcid:
        return
    _sub(
        parent_el,
        "nameIdentifier",
        orcid if orcid.startswith("http") else f"https://orcid.org/{orcid}",
        nameIdentifierScheme="ORCID",
        schemeURI="https://orcid.org",
    )


def to_datacite_oai_xml(record: HarvestedRecord) -> str:
    """
    Serialise a `HarvestedRecord` to a pretty-printed OAI-PMH <record> that
    contains a DataCite <resource>.

    Optional elements are only written when the corresponding data is present.
    <identifier>, <titles>, <publisher> and <resourceType> are always written.
    """
    meta = record.metadata

    ET.register_namespace("", OAI_NS)
    ET.register_namespace("xsi", XSI_NS)

    # OAI-PMH record root
    oai_record = ET.Element("record", {"xmlns": OAI_NS, "xmlns:xsi": XSI_NS})

    header = ET.SubElement(oai_record, "header", {"status": "deleted"} if record.is_deleted else {})
    _sub(header, "identifier", record.record_id)
    _sub(header, "datestamp", record.datestamp)

    metadata = ET.SubElement(oai_record, "metadata")
    resource = ET.SubElement(
        metadata,
        "resource",
        {
            "xmlns": DATACITE_NS,
            "xmlns:xsi": XSI_NS,
            "xsi:schemaLocation": DATACITE_SCHEMA_LOCATION,
        },
    )

    # IDENTIFIER (mandatory)
    _sub(resource, "identifier", meta.identifier, identifierType=meta.identifier_type)

    # CREATORS (mandatory)
    if meta.creators:
        creators = ET.SubElement(resource, "creators")
        for person in meta.creators:
            creator = ET.SubElement(creators, "creator")
            _sub(creator, "creatorName", person.name, nameType=person.name_type)
            add_orcid(creator, person.orcid)
            if person.affiliation:
                _sub(creator, "affiliation", person.affiliation)

    # TITLES (mandatory)
    titles = ET.SubElement(resource, "titles")
    _sub(titles, "title", meta.title)

    # PUBLISHER (mandatory)
    _sub(resource, "publisher", meta.publisher)

    # PUBLICATION YEAR (mandatory)
    if meta.publication_year:
        _sub(resource, "publicationYear", meta.publication_year)

    # RESOURCE TYPE (mandatory)
    _sub(resource, "resourceType", meta.resource_type, resourceTypeGeneral=meta.resource_type_general)

    # LANGUAGE
    if meta.language:
        _sub(resource, "language", meta.language)

    # CONTRIBUTORS
    if meta.contributors:
        contributors = ET.SubElement(resource, "contributors")
        for c in meta.contributors:
            contributor = _sub(contributors, "contributor", contributorType=c.contributor_type)
            _sub(contributor, "contributorName", c.person.name, nameType=c.person.name_type)
            add_orcid(contributor, c.person.orcid)
            if c.person.affiliation:
                _sub(contributor, "affiliation", c.person.affiliation)

    # DATES
    if meta.dates:
        dates = ET.SubElement(resource, "dates")
        for d in meta.dates:
            _sub(dates, "date", d.value, dateType=d.date_type)

    # RELATED IDENTIFIERS
    if meta.related_identifiers:
        related = ET.SubElement(resource, "relatedIdentifiers")
        for r in meta.related_identifiers:
            _sub(related, "relatedIdentifier", r.value, relatedIdentifierType=r.id_type, relationType=r.relation)

    # SIZES
    if meta.sizes:
        sizes_el = ET.SubElement(resource, "sizes")
        for size in meta.sizes:
            _sub(sizes_el, "size", size)

    # SUBJECTS
    if meta.subjects:
        subjects_el = ET.SubElement(resource, "subjects")
        for subject in meta.subjects:
            _sub(subjects_el, "subject", subject)

    # FORMATS
    if meta.formats:
        formats_el = ET.SubElement(resource, "formats")
        for fmt in meta.formats:
            _sub(formats_el, "format", fmt)

    # RIGHTS
    if meta.rights:
        rights_list = ET.SubElement(resource, "rightsList")
        for rights in meta.rights:
            attrs = {}
            if rights.uri:
                attrs["rightsURI"] = rights.uri
            if rights.identifier:
                attrs["rightsIdentifier"] = rights.identifier
            _sub(rights_list, "rights", rights.text, **attrs)

    # DESCRIPTIONS
    if meta.descriptions:
        descriptions = ET.SubElement(resource, "descriptions")
        for desc in meta.descriptions:
            _sub(descriptions, "description", desc.text, descriptionType=desc.description_type)

    # FUNDING REFERENCES
    if meta.funding_references:
        funding_refs = ET.SubElement(resource, "fundingReferences")
        for grant in meta.funding_references:
            funding_ref = ET.SubElement(funding_refs, "fundingReference")
            _sub(funding_ref, "funderName", grant.funder_name)
            if grant.award_number:
                _sub(funding_ref, "awardNumber", grant.award_number)

    xml_str = ET.tostring(oai_record, encoding="unicode")
    return minidom.parseString(xml_str).toprettyxml(indent="  ")
