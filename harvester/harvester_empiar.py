import logging, time
from typing import Any, Iterator, cast

from .core import (
    CommonRecord,
    Contributor,
    DateEntry,
    Description,
    FundingReference,
    HarvestedRecord,
    Person,
    RelatedIdentifier,
    Rights,
    SourceAdapter,
    get_json,
    make_client,
    run_harvester,
    to_datacite_oai_xml,
)

logger = logging.getLogger(__name__)

_EMPIAR_CLIENT = make_client(120)
DEFAULT_BASE_URL = "https://www.ebi.ac.uk"
SEARCH_PATH = "/emdb/api/empiar/search/{query}"
PAGE_SIZE = 100
PAGE_DELAY_SECONDS = 2


def search_page(page: int, query: str) -> dict[str, Any]:
    """
    Fetch one page of results from `GET /emdb/api/empiar/search/{query}`.
 
    Queries the EMPIAR search endpoint and requests all fields (`fl=*`) for the
    given page, using the module-level `PAGE_SIZE` as the row count.
 
    :param page: The number of page to fetch.
    :param query: The search query.
 
    :return: The parsed JSON payload for that page.
    """
    url = DEFAULT_BASE_URL + SEARCH_PATH.format(query = query)
    params: dict[str, Any] = {"rows": PAGE_SIZE, "page": page, "fl": "*"}
    return cast(dict[str, Any], get_json(_EMPIAR_CLIENT, url, params = params))



def _search_pages(query: str) -> Iterator[dict[str, Any]]:
    """
    Walk every page of the EMPIAR search endpoint for `query` and yield each
    page's payload as it's fetched. Stops at the first empty page.
    """
    page = 1
    total_records = 0
    while True:
        payload = search_page(page, query)

        if not payload:
            logger.info("Page %d empty, stopping", page)
            break

        total_records += len(payload)
        logger.info(
            "Page %d: %d records (%d total so far)",
            page, len(payload), total_records,
        )
        yield payload

        page += 1
        time.sleep(PAGE_DELAY_SECONDS)



def search_all() -> Iterator[dict[str, Any]]:
    """
    Walk every page of the EMPIAR search endpoint and yield each page's
    payload as it's fetched.

    :return: An iterator over page payloads, where each payload is a dict
            mapping EMPIAR IDs to their metadata records.
    """
    return _search_pages("*")



def search_incremental(from_date: str, until_date: str) -> Iterator[dict[str, Any]]:
    """
    Walk every page of the EMPIAR search endpoint for entries updated within
    a date range, and yield each page's payload as it's fetched.

    :param from_date: The start of the update-date range.
    :param until_date: The end of the update-date range.
    :return: An iterator over page payloads, where each payload is a dict
        mapping EMPIAR IDs to their metadata records.
    """
    return _search_pages(f"database:EMPIAR AND update_date:[{from_date} TO {until_date}]")



def empiar_additional_file_metadata(entry_id:  str) -> list[dict[str, Any]]:
    """
    Fetch the image-set metadata for a single EMPIAR entry

    :params: entry_id: the numeric EMPIAR id (e.g. "10050"), without the "EMPIAR-" prefix
    :return: the list of imagesets dictionaries as returned by the EMPIAR API
    """
    empiar_entry_endpoint = f"https://www.ebi.ac.uk/empiar/api/entry/EMPIAR-{entry_id}/"
    payload = get_json(_EMPIAR_CLIENT, empiar_entry_endpoint)
    return cast(list[dict[str, Any]], payload[f"EMPIAR-{entry_id}"]["imagesets"])



def add_file_sizes(empiar_id: str, additional_file_metadata: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Adds the byte size of each additional file collection to its metadata.

    :param empiar_id: The EMPIAR accession identifier.
    :param additional_file_metadata: A list of metadata dictionaries describing the additional file collections.
    :return: The input list of metadata dictionaries, with each dictionary containing the added byte_size field.
    """
    empiar_entry_endpoint = f"https://www.ebi.ac.uk/empiar/{empiar_id}/dirStruct%3Dfull&list%3D1/"
    for index, metadata in enumerate(additional_file_metadata):
        try:
            payload = get_json(_EMPIAR_CLIENT, f"{empiar_entry_endpoint}{index}/")
            metadata["byte_size"] = payload[0]["size"]
        except Exception as e:
            logger.error("Error while fetching additional file size: %s", e)
    
    return additional_file_metadata



def extract_keywords_from_emdb_references(record: dict[str, Any]) -> set[str]:
    """
    Extract useful descriptive keywords from linked EMDB entries.

    :param record: An EMPIAR record containing a ``cross_references`` field
            with references to associated EMDB entries.

    :return: A set of unique descriptive keywords extracted from the linked
            EMDB entries.
    """

    keywords = set[str]()

    def add(value: str) -> None:
        if value:
            keywords.update(x.strip() for x in str(value).split(",") if x.strip())

    # EMDB IDs from EMPIAR record
    emdb_ids = [x.get("name") if isinstance(x, dict) else x for x in record.get("cross_references", [])]

    for emdb_id in filter(None, emdb_ids):
        try:
            data = _EMPIAR_CLIENT.get(f"https://www.ebi.ac.uk/emdb/api/entry/{emdb_id}", timeout = 10).json()
        except Exception:
            continue

        admin = data.get("admin", {})
        add(admin.get("keywords"))
        for s in data.get("structure_determination_list", {}).get("structure_determination", []):
            add(s.get("method"))

    return keywords



def empiar_to_common(entry_id: str, meta: dict[str, Any], keywords: set[str]) -> CommonRecord:
    """
    Map an EMPIAR entry record onto the repository-neutral DataCite-shaped
    `CommonRecord`.

    :param entry_id: The numeric EMPIAR id (e.g. "12345"), without the "EMPIAR-" prefix.
    :param meta: Entry dictionary obtained from the EMPIAR API.
    :param keywords: Keywords gathered from the linked EMDB entries.
    """
    creation_date = meta.get("deposition_date") or ""
    update_date = meta.get("update_date") or ""
    release_date = meta.get("release_date") or ""

    # IDENTIFIER (mandatory)
    doi = meta.get("entry_doi") or ""
    doi_value = doi.split("doi:")[-1] if doi.lower().startswith("doi:") else doi

    if doi_value:
        identifier, identifier_type = doi_value, "DOI"
    else:
        identifier, identifier_type = f"https://www.ebi.ac.uk/empiar/EMPIAR-{entry_id}/", "URL"

    # CREATORS (mandatory)
    # Priority:
    # 1. EMPIAR authors
    # 2. Principal investigators
    # 3. Citation authors

    author_entries = []

    for a in (meta.get("authors") or []):
        author = a.get("author", {})
        if author.get("name"):
            author_entries.append((author["name"], author.get("author_orcid"), author.get("organization")))

    if not author_entries:
        author_entries = [
            (
                f"{pi.get('last_name', '')}, {pi.get('first_name', '')}".strip(", "),
                pi.get("author_orcid"),
                pi.get("organization")
            )
            for pi in (meta.get("principal_investigator") or [])
            if pi.get("last_name") or pi.get("first_name")
        ]

    if not author_entries:
        for cit in meta.get("citation") or []:
            author_entries = [
                (a.get("name"), a.get("author_orcid"), None)
                for a in (cit.get("authors") or [])
                if a.get("name")
            ]
            if author_entries:
                break

    creators = [Person(name = name, orcid = orcid, affiliation = affiliation) for name, orcid, affiliation in author_entries]

    # LANGUAGE
    language = None
    languages = {c.get("language") for c in (meta.get("citation") or []) if c.get("language")}
    if languages:
        lang_map = {"english": "en"}
        raw_lang = sorted(languages)[0]
        language = lang_map.get(raw_lang.lower(), raw_lang)

    # CONTRIBUTORS
    pi_list = meta.get("principal_investigator") or []
    corr = (meta.get("corresponding_author") or {}).get("author")

    contributors_data = []
    seen_names = set()
    for person in pi_list:
        name = f"{person.get('last_name', '')}, {person.get('first_name', '')}".strip(", ")
        if name and name not in seen_names:
            seen_names.add(name)
            contributors_data.append((name, person, "ProjectLeader"))

    if corr:
        name = f"{corr.get('last_name', '')}, {corr.get('first_name', '')}".strip(", ")
        if name and name not in seen_names:
            contributors_data.append((name, corr, "ContactPerson"))

    contributors = [
        Contributor(
            person = Person(name = name, orcid = person.get("author_orcid"), affiliation = person.get("organization")),
            contributor_type = role,
        )
        for name, person, role in contributors_data
    ]

    # DATES
    dates = []
    if creation_date:
        dates.append(DateEntry("Submitted", creation_date[:10]))
    if release_date:
        dates.append(DateEntry("Available", release_date[:10]))
    if update_date:
        dates.append(DateEntry("Updated", update_date[:10]))

    # RELATED IDENTIFIERS
    related_items = []

    # EMDB maps are derived from EMPIAR data
    for xref in meta.get("cross_references") or []:
        emd_id = xref.get("name") if isinstance(xref, dict) else xref

        if emd_id:
            related_items.append(RelatedIdentifier("URL", "IsSourceOf", f"https://www.ebi.ac.uk/emdb/{emd_id}"))

    # Publications documenting the dataset
    for cit in meta.get("citation") or []:

        if cit.get("doi"):
            cit_doi = cit["doi"]
            cit_doi_value = (
                cit_doi.split("doi:")[-1]
                if cit_doi.lower().startswith("doi:")
                else cit_doi
            )

            related_items.append(RelatedIdentifier("DOI", "IsDocumentedBy", cit_doi_value))

        if cit.get("pubmedid"):
            related_items.append(RelatedIdentifier("URL", "IsDocumentedBy", f"https://pubmed.ncbi.nlm.nih.gov/{cit['pubmedid']}/"))

    # SIZES
    dataset_size = meta.get("dataset_size")

    # FORMATS
    formats_set = {
        img[key]
        for img in (meta.get("imagesets") or [])
        for key in ("header_format", "data_format")
        if img.get(key)
    }

    # DESCRIPTIONS - built entirely from imageset name + details
    descriptions = []
    for img in meta.get("imagesets") or []:
        name = img.get("name")
        details = img.get("details")
        if name and details:
            descriptions.append(Description(f"{name}: {details}", "TechnicalInfo"))
        elif name or details:
            descriptions.append(Description(name or details, "TechnicalInfo"))

    # FUNDING REFERENCES
    funding = [
        FundingReference(funder_name = g["funding_body"], award_number = g.get("code"))
        for g in (meta.get("grant_references") or [])
        if g.get("funding_body")
    ]

    pub_year_source = release_date or creation_date

    return CommonRecord(
        identifier = identifier,
        identifier_type = identifier_type,
        title = meta.get("title"),
        publisher = "EMBL-European Bioinformatics Institute",
        publication_year = pub_year_source[:4] if pub_year_source else None,
        resource_type = "Electron microscopy dataset",
        resource_type_general = "Dataset",
        creators = creators,
        language = language,
        contributors = contributors,
        dates = dates,
        related_identifiers = related_items,
        sizes = [dataset_size] if dataset_size else [],
        subjects = sorted(keywords),
        formats = sorted(formats_set),
        rights = [
            Rights(
                text = "CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
                uri = "https://creativecommons.org/publicdomain/zero/1.0/",
                identifier = "CC0-1.0",
            )
        ],
        descriptions = descriptions,
        funding_references = funding,
    )



def empiar_data_to_datacite(entry_id: str, record: dict[str, Any], keywords: set[str]) -> tuple[str, Any | str]:
    """
    Convert an EMPIAR entry record into a DataCite 4.6 XML record wrapped in
    an OAI-PMH <record> element.

    :param entry_id: The numeric EMPIAR id (e.g. "12345"), without the "EMPIAR-" prefix.
    :param record: Entry dictionary obtained from the EMPIAR API

    :return: A tuple containing:
            - xml_pretty (str): Formatted DataCite XML record.
            - datestamp (str): Record update/creation date (YYYY-MM-DD).
    """
    harvested = HarvestedRecord(
        record_id = f"EMPIAR-{entry_id}",
        datestamp = _datestamp(record),
        metadata = empiar_to_common(entry_id, record, keywords),
        is_deleted = bool(record.get("obsolete_date")),
    )
    return to_datacite_oai_xml(harvested), harvested.datestamp



def _datestamp(record: dict[str, Any]) -> str:
    return ((record.get("update_date") or "") or (record.get("deposition_date") or ""))[:10]



class EmpiarAdapter(SourceAdapter):
    name = "EMPIAR"

    def iter_raw(self, from_date: str | None, until_date: str) -> Iterator[tuple[str, dict[str, Any]]]:
        """Yield `(empiar_key, record)` pairs, e.g. `("EMPIAR-10050", {...})`."""
        pages = search_all() if not from_date else search_incremental(from_date, until_date)
        for page in pages:
            yield from page.items()

    def build_record(self, raw: tuple[str, dict[str, Any]]) -> HarvestedRecord:
        empiar_key, record = raw
        entry_id = empiar_key.removeprefix("EMPIAR-")

        keywords = extract_keywords_from_emdb_references(record)
        metadata = empiar_to_common(entry_id, record, keywords)

        additional_file_metadata = empiar_additional_file_metadata(entry_id)
        additional_file_metadata_updated = add_file_sizes(empiar_key, additional_file_metadata)

        return HarvestedRecord(
            record_id = empiar_key,
            datestamp = _datestamp(record),
            metadata = metadata,
            additional_metadata = additional_file_metadata_updated,
            is_deleted = bool(record.get("obsolete_date")),
        )



def run_harvester_empiar(run_info: dict[str, Any]) -> bool:
    """
    Run a full (or incremental) EMPIAR harvest and push
    each entry to the data warehouse as a harvest event.
 
    :param run_info: Dictionary describing the harvest run.
 
    :return: `True` if every harvest event was sent successfully (and no
            unexpected exception occurred), `False` if any event failed to
            send or an exception was raised during the run.
    """
    return run_harvester(run_info, EmpiarAdapter())