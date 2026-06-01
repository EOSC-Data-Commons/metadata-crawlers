import logging, os, requests, xml.etree.ElementTree as ET, time, mimetypes
from datetime import datetime
from .db_api_functions import send_harvest_event
from xml.dom import minidom

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))



def guess_format(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename)

    # fallback for unknown scientific formats
    if mime is None:
        ext = filename.lower().split(".")[-1]
        custom_map = {
            "pdb": "chemical/x-pdb",
            "prmtop": "application/octet-stream",
            "xtc": "application/octet-stream",
            "bin": "application/octet-stream",
            "topology": "application/octet-stream",
        }
        return custom_map.get(ext, "application/octet-stream")
    return mime



def fetch_projects_summary(base_api_url, headers) -> dict:
    url = f"{base_api_url}/projects/summary"
    response = requests.get(url, headers = headers, timeout = 30)
    response.raise_for_status()
    return response.json()



def fetch_projects_data(base_api_url: str, from_date, headers) -> list:
    """
    Fetch all projects from the MDposit API using pagination.
    """
    project_summary = fetch_projects_summary(base_api_url, headers)
    total = project_summary["projectsCount"]
    print(f"Total projects: {total}")

    projects = []
    page = 1

    while len(projects) < 100:
        response = requests.get(
            f"{base_api_url}/projects",
            headers = headers,
            params = {"limit": 100, "page": page},
            timeout = 30
        )
        response.raise_for_status()
        data = response.json()
        projects.extend(data["projects"])
        print(f"Fetched page {page}, total so far: {len(projects)}")
        page += 1
        time.sleep(1)

    if from_date:
        filtered_projects = [
            p for p in projects
            if p.get("creationDate") and datetime.fromisoformat(p["creationDate"].replace("Z", "+00:00")) > datetime.fromisoformat(from_date.replace("Z", "+00:00"))
        ]
        return filtered_projects
    else:
        return projects



def build_description(project) -> str:
    meta = project["metadata"]
    sentences = []

    # METHOD
    method = meta.get("METHOD")
    if method:
        sentences.append(f"The dataset was generated using {method}.")

    # SYSKEYS
    syskeys = meta.get("SYSKEYS") or []
    if syskeys:
        sentences.append(f"he system is composed of the following components: {', '.join(syskeys)}.")

    # DOMAINS
    domains = meta.get("DOMAINS") or []
    if domains:
        clean_domains = ", ".join(domains)
        sentences.append(
            f"The system relates to the following domains: {clean_domains}."
        )

    # ANALYSES
    analyses = project.get("analyses") or []
    if analyses:
        pretty = ", ".join(analyses)
        sentences.append(
            f"The dataset includes the following analyses: {pretty}."
        )

    return " ".join(sentences)



def mdposit_data_to_datacite(project: dict):
    meta = project["metadata"]

    # NAMESPACES
    ET.register_namespace("", "http://www.openarchives.org/OAI/2.0/")
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
    ET.register_namespace("dc", "http://datacite.org/schema/kernel-4")

    # DATES
    creation_date = project.get("creationDate", "")
    update_date = project.get("updateDate", "")

    # OAI-PMH RECORD ROOT
    record = ET.Element(
        "record",
        {
            "xmlns": "http://www.openarchives.org/OAI/2.0/",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        },
    )

    # HEADER
    header = ET.SubElement(record, "header")

    header_identifier = ET.SubElement(header, "identifier")
    header_identifier.text = project['accession']

    datestamp = ET.SubElement(header, "datestamp")
    datestamp.text = update_date[:10] if update_date else creation_date[:10]

    # METADATA BLOCK
    metadata = ET.SubElement(record, "metadata")

    # DATACITE RESOURCE
    resource = ET.SubElement(
        metadata,
        "resource",
        {
            "xmlns": "http://datacite.org/schema/kernel-4",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": (
                "http://datacite.org/schema/kernel-4 "
                "https://schema.datacite.org/meta/kernel-4.6/metadata.xsd"
            ),
        },
    )

    # IDENTIFIER
    identifier = ET.SubElement(resource, "identifier", identifierType = "URL")
    identifier.text = f"https://mdposit.mddbr.eu/#/browse/{project['accession']}"

    # CREATORS
    creators = ET.SubElement(resource, "creators")
    for author in meta.get("AUTHORS") or []:
        creator = ET.SubElement(creators, "creator")
        creator_name = ET.SubElement(creator, "creatorName", nameType = "Personal")
        creator_name.text = author

    # TITLES
    titles = ET.SubElement(resource, "titles")
    ET.SubElement(titles, "title").text = meta.get("NAME", "")

    # PUBLISHER
    ET.SubElement(resource, "publisher").text = "MDposit"

    # PUBLICATION YEAR
    ET.SubElement(resource, "publicationYear").text = creation_date[:4]

    # RESOURCE TYPE
    ET.SubElement(resource, "resourceType", resourceTypeGeneral="Dataset").text = "Molecular Dynamics Trajectory"

    # CONTRIBUTORS
    if meta.get("CONTACT"):
        contributors = ET.SubElement(resource, "contributors")
        contributor = ET.SubElement(contributors, "contributor", contributorType = "ContactPerson")
        ET.SubElement(contributor, "contributorName").text = meta["CONTACT"]

    # DATES
    dates = ET.SubElement(resource, "dates")
    if creation_date:
        ET.SubElement(dates, "date", dateType="Created").text = creation_date[:10]

    if update_date:
        ET.SubElement(dates, "date", dateType="Updated").text = update_date[:10]

    # RELATED IDENTIFIERS
    related = ET.SubElement(resource, "relatedIdentifiers")
    # PDB
    for pdbid in meta.get("PDBIDS") or []:
        ET.SubElement(related, "relatedIdentifier", relatedIdentifierType = "URL", relationType = "IsDerivedFrom").text = f"https://www.rcsb.org/structure/{pdbid}"

    # UniProt
    for ref in meta.get("REFERENCES") or []:
        ET.SubElement(related, "relatedIdentifier", relatedIdentifierType = "URL", relationType = "References").text = f"https://www.uniprot.org/uniprot/{ref}"

    # DOI
    citation = meta.get("CITATION") or ""
    if citation:
        if "https://doi.org/" in citation:
            doi = citation.split("https://doi.org/")[-1].strip()
            ET.SubElement(related, "relatedIdentifier", relatedIdentifierType="DOI", relationType="IsDocumentedBy").text = doi
        elif "DOI:" in citation:
            doi = citation.replace("DOI:", "").strip()
            ET.SubElement(related, "relatedIdentifier", relatedIdentifierType="DOI", relationType="IsDocumentedBy").text = doi

    # FORMATS
    files = project.get("files") or []
    formats_set = {guess_format(f) for f in files if "." in f}
    formats_el = ET.SubElement(resource, "formats")
    for fmt in sorted(formats_set):
        ET.SubElement(formats_el, "format").text = fmt

    # RIGHTS
    rights_list = ET.SubElement(resource, "rightsList")
    if meta.get("LINKCENSE") and meta.get("LICENSE"):
        ET.SubElement(rights_list, "rights", rightsURI = meta.get("LINKCENSE"), rightsIdentifier = "CC-BY-4.0").text = meta.get("LICENSE")

    # DESCRIPTIONS
    descriptions = ET.SubElement(resource, "descriptions")
    description_text = build_description(project)
    if meta.get("DESCRIPTION"):
        description_text = meta["DESCRIPTION"].strip() + " " + description_text
    ET.SubElement(descriptions, "description", descriptionType="Abstract").text = description_text

    xml_str = ET.tostring(record, encoding="unicode")
    xml_pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")
    return xml_pretty, identifier.text, (update_date or creation_date)[:10]



def run_harvester_mdposit(run_info: dict) -> bool:

    try:
        record_count = 0
        harvest_events = 0
        failed_events = 0

        config = run_info.get("endpoint_config")
        harvest_url = config.get("harvest_url")
        from_date = run_info.get("from_date")
        headers = {"Accept": "application/json"}

        mdposit_data_projects = fetch_projects_data(harvest_url, from_date, headers)
        for project in mdposit_data_projects:
            mdposit_xml, identifier, datestamp = mdposit_data_to_datacite(project)
            additional_file_metadata = ", ".join(project.get("files", []))

            event_payload = {
                "record_identifier": identifier,
                "datestamp": datestamp,
                "raw_metadata": mdposit_xml,
                "additional_metadata": additional_file_metadata,
                "harvest_url": harvest_url,
                "repo_code": "MDDB",
                "harvest_run_id": run_info.get("id"),
                "is_deleted": False
            }

            if send_harvest_event(event_payload):
                harvest_events += 1
            else:
                failed_events += 1

        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )

        if failed_events == 0:
            return True
        else:
            return False
            
    except Exception as e:
        logger.exception("Unexpected error in run_harvester_oaipmh: %s", e)
        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )
        return False