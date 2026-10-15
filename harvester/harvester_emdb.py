import logging, time
from datetime import datetime
from typing import Any, Iterator, cast

from .core import (
    CommonRecord,
    DateEntry,
    Description,
    FundingReference,
    HarvestedRecord,
    Person,
    RelatedIdentifier,
    Rights,
    SourceAdapter,
    as_list,
    get_json,
    make_client,
    run_harvester,
    strip_doi,
    text,
)

_EMDB_CLIENT = make_client(120)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://www.ebi.ac.uk"
SEARCH_PATH = "/emdb/api/search/{query}"
ENTRY_PATH = "/emdb/api/entry/{emdb_id}"
PAGE_SIZE = 100
PAGE_DELAY_SECONDS = 2
INCREMENTAL_DATE_FIELD = "update_date"
DELETED_STATUS_CODES = {"OBS"}
MAX_SUBJECT_LENGTH = 100



# ---------------------------------------------------------------------------
# Entry point and adapter
# ---------------------------------------------------------------------------

def run_harvester_emdb(run_info: dict[str, Any]) -> bool:
    """
    Run a full (or incremental) EMDB harvest and push each entry to the data
    warehouse as a harvest event.

    :param run_info: Dictionary describing the harvest run.
    :return: `True` if every harvest event was sent successfully, `False` otherwise.
    """
    return run_harvester(run_info, EmdbAdapter())



class EmdbAdapter(SourceAdapter):
    name = "EMDB"

    def iter_raw(self, from_date: str | None, until_date: str) -> Iterator[tuple[str, dict[str, Any]]]:
        """Yield `(emdb_id, entry)` for every hit, e.g. `("EMD-1234", {...full entry JSON...})`."""
        if not from_date:
            yield from search_all()
        else:
            yield from search_incremental(from_date, until_date)

    def build_record(self, raw: tuple[str, dict[str, Any]]) -> HarvestedRecord:
        """Build one record from a search hit: DataCite metadata, datestamp, files."""
        emdb_id, entry = raw
        if "admin" not in entry:
            raise ValueError(f"Search hit for {emdb_id} is not a full entry (no 'admin' section); check SEARCH_FIELDS")

        return HarvestedRecord(
            record_id = emdb_id,
            datestamp = emdb_datestamp(entry),
            metadata = emdb_to_common(emdb_id, entry),
            additional_metadata = emdb_files(entry),
            is_deleted = emdb_is_deleted(entry),
        )



# ---------------------------------------------------------------------------
# API access: which entries exist, and the full JSON of one entry
# ---------------------------------------------------------------------------

def search_all() -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield `(emdb_id, entry)` for every EMDB entry."""
    return _search_entries("*")



def search_incremental(from_date: str, until_date: str) -> Iterator[tuple[str, dict[str, Any]]]:
    """
    Yield `(emdb_id, entry)` for entries whose `INCREMENTAL_DATE_FIELD` falls in the range.

    :param from_date: Start of the range, an ISO timestamp (e.g. 2026-10-13T07:27:53.494538Z).
    :param until_date: End of the range, same format.
    """
    return _search_entries(
        f"{INCREMENTAL_DATE_FIELD}:[{_solr_date(from_date)} TO {_solr_date(until_date)}]"
    )



def fetch_entry(emdb_id: str) -> dict[str, Any]:
    """
    Fetch the full entry JSON for one EMDB id (e.g. "EMD-1234"). The harvest does
    not use this (search hits already are full entries); it is for looking at a
    single entry, e.g. to re-check a record that failed.
    """
    url = DEFAULT_BASE_URL + ENTRY_PATH.format(emdb_id = emdb_id)
    entry = get_json(_EMDB_CLIENT, url)
    if isinstance(entry, dict) and isinstance(entry.get(emdb_id), dict):  # tolerate {"EMD-1234": {...}}
        entry = entry[emdb_id]
    return cast(dict[str, Any], entry)



def search_page(page: int, query: str) -> Any:
    """
    Fetch one page from `GET /emdb/api/search/{query}`.

    :param page: The page number to fetch (1-based).
    :param query: The Solr query.
    :return: The parsed JSON payload for that page.
    """
    url = DEFAULT_BASE_URL + SEARCH_PATH.format(query = query)
    params: dict[str, Any] = {"rows": PAGE_SIZE, "page": page}
    return get_json(_EMDB_CLIENT, url, params = params)



def _search_entries(query: str) -> Iterator[tuple[str, dict[str, Any]]]:
    """
    Walk every page for `query` and yield `(emdb_id, entry)` for each hit. Stops at
    the first empty page. If a page contains only entries we have already seen,
    pagination is not advancing (wrong paging parameters), so it raises instead of
    looping forever.
    """
    page = 1
    seen = set[str]()
    while True:
        hits = extract_entries(search_page(page, query))

        if not hits:
            logger.info("Page %d empty, stopping", page)
            break

        new_hits = [(emdb_id, entry) for emdb_id, entry in hits if emdb_id not in seen]
        if not new_hits:
            raise RuntimeError(f"Page {page} returned only already-seen ids: paging parameters are probably wrong")

        seen.update(emdb_id for emdb_id, _ in new_hits)
        logger.info("Page %d: %d records (%d total so far)", page, len(new_hits), len(seen))
        yield from new_hits

        page += 1
        time.sleep(PAGE_DELAY_SECONDS)



def extract_entries(payload: Any) -> list[tuple[str, dict[str, Any]]]:
    """
    Read one search page: a JSON array of full entries, each with an `emdb_id`.
    Returns `(emdb_id, entry)` pairs.

    An empty page returns `[]`. Anything else raises, so a change in the API fails
    loudly instead of "successfully" harvesting nothing.
    """
    if not payload:
        return []

    if not isinstance(payload, list):
        keys = list(payload)[:10] if isinstance(payload, dict) else None
        raise ValueError(f"Expected a list of entries from the EMDB search, got {type(payload).__name__} (keys: {keys})")

    hits = []
    for hit in payload:
        emdb_id = hit.get("emdb_id") if isinstance(hit, dict) else None
        if not isinstance(emdb_id, str) or not emdb_id:
            raise ValueError(f"EMDB search hit without an 'emdb_id': {str(hit)[:200]}")
        hits.append((emdb_id, hit))
    return hits



def _solr_date(value: str) -> str:
    """
    Convert a run timestamp to the format the EMDB Solr range query accepts:
    whole seconds and a trailing Z.

    2026-10-13T07:27:53.494538Z -> 2026-10-13T07:27:53Z
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%dT%H:%M:%SZ")



# ---------------------------------------------------------------------------
# Mapping: EMDB entry JSON -> DataCite fields (CommonRecord)
# ---------------------------------------------------------------------------

def emdb_to_common(emdb_id: str, entry: dict[str, Any]) -> CommonRecord:
    """
    Map an EMDB entry JSON onto the DataCite-shaped `CommonRecord`. Each DataCite
    property is built by its own small function below.

    :param emdb_id: e.g. "EMD-1234".
    :param entry: Entry dictionary from `/emdb/api/entry/{id}`.
    """
    admin = entry.get("admin") or {}
    deposition, release, _ = _key_dates(entry)
    publication_date = release or deposition
    subjects, long_keywords = _subjects(entry)

    return CommonRecord(
        # EMDB entries carry no DOI of their own, so the identifier is the entry URL.
        identifier = f"https://www.ebi.ac.uk/emdb/{emdb_id}",
        identifier_type = "URL",
        title = text(admin.get("title")),
        publisher = "EMBL-European Bioinformatics Institute",
        publication_year = publication_date[:4] if publication_date else None,
        resource_type = "Electron microscopy map",
        resource_type_general = "Dataset",
        creators = _creators(entry),
        dates = _dates(entry),
        related_identifiers = _related_identifiers(entry, emdb_id),
        subjects = subjects,
        rights = [
            Rights(
                text = "CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
                uri = "https://creativecommons.org/publicdomain/zero/1.0/",
                identifier = "CC0-1.0",
            )
        ],
        descriptions = _descriptions(entry, long_keywords),
        funding_references = _funding(entry),
    )



def emdb_datestamp(entry: dict[str, Any]) -> str:
    """The record's datestamp (YYYY-MM-DD): last update, else release, else deposition."""
    deposition, release, update = _key_dates(entry)
    return (update or release or deposition)[:10]



def emdb_is_deleted(entry: dict[str, Any]) -> bool:
    """True if the entry's status code is one of `DELETED_STATUS_CODES`."""
    status = ((entry.get("admin") or {}).get("current_status") or {}).get("code")
    return text(status) in DELETED_STATUS_CODES



def _key_dates(entry: dict[str, Any]) -> tuple[str, str, str]:
    """
    Return (deposition, release, update) as timestamps like "2026-10-07T00:00:00",
    or "" when missing. Release is `map_release`, else `header_release`.
    """
    key_dates = (entry.get("admin") or {}).get("key_dates") or {}
    deposition = text(key_dates.get("deposition")) or ""
    release = text(key_dates.get("map_release")) or text(key_dates.get("header_release")) or ""
    update = text(key_dates.get("update")) or ""
    return deposition, release, update



def _creators(entry: dict[str, Any]) -> list[Person]:
    """
    The entry's own authors (`admin.authors_list`); if there are none, the
    primary citation's authors. Authors may carry an ORCID.
    """
    entry_authors = ((entry.get("admin") or {}).get("authors_list") or {}).get("author")
    return _people(entry_authors) or _people(_primary_citation(entry).get("author"))



def _people(authors: Any) -> list[Person]:
    """Turn a list of author JSON items (`{"ORCID": ..., "valueOf_": "Appel L"}`) into `Person`s."""
    people = []
    for a in as_list(authors):
        name = text(a)
        if name:
            orcid = text(a.get("ORCID") or a.get("@ORCID")) if isinstance(a, dict) else None
            people.append(Person(name = name, orcid = orcid))
    return people



def _dates(entry: dict[str, Any]) -> list[DateEntry]:
    """DataCite dates: Submitted (deposition), Available (release), Updated, as YYYY-MM-DD."""
    deposition, release, update = _key_dates(entry)
    dates = []
    if deposition:
        dates.append(DateEntry("Submitted", deposition[:10]))
    if release:
        dates.append(DateEntry("Available", release[:10]))
    if update:
        dates.append(DateEntry("Updated", update[:10]))
    return dates



def _related_identifiers(entry: dict[str, Any], emdb_id: str) -> list[RelatedIdentifier]:
    """
    Links to other resources: the primary publication (IsDocumentedBy), other
    EMDB entries of the same study (References; the entry itself is skipped) and
    the atomic models fitted into the map (IsSourceOf).
    """
    related = []

    doi, pubmed = _citation_ids(_primary_citation(entry))
    if doi:
        related.append(RelatedIdentifier("DOI", "IsDocumentedBy", doi))
    if pubmed:
        related.append(RelatedIdentifier("URL", "IsDocumentedBy", f"https://pubmed.ncbi.nlm.nih.gov/{pubmed}/"))

    crossreferences = entry.get("crossreferences") or {}

    for ref in as_list((crossreferences.get("emdb_list") or {}).get("emdb_reference")):
        related_id = text(ref.get("emdb_id")) if isinstance(ref, dict) else text(ref)
        if related_id and related_id != emdb_id:
            related.append(RelatedIdentifier("URL", "References", f"https://www.ebi.ac.uk/emdb/{related_id}"))

    for ref in as_list((crossreferences.get("pdb_list") or {}).get("pdb_reference")):
        pdb_id = text(ref.get("pdb_id")) if isinstance(ref, dict) else text(ref)
        if pdb_id:
            related.append(RelatedIdentifier("URL", "IsSourceOf", f"https://www.ebi.ac.uk/pdbe/entry/pdb/{pdb_id.lower()}"))

    return related



def _primary_citation(entry: dict[str, Any]) -> dict[str, Any]:
    """
    The main publication of the entry (`crossreferences.citation_list.primary_citation`).
    Its title, authors and `external_references` sit directly under `citation_type`;
    the journal_citation / non_journal_citation sub-keys are a fallback.
    """
    cit = ((entry.get("crossreferences") or {}).get("citation_list") or {}).get("primary_citation") or {}
    cit_type = cit.get("citation_type") or {}
    return cit_type.get("journal_citation") or cit_type.get("non_journal_citation") or cit_type



def _citation_ids(cit: dict[str, Any]) -> tuple[str | None, str | None]:
    """
    Return (doi, pubmed_id) of a citation, or None for each one that is missing.

    They are listed in `external_references`, e.g.
        {"type_": "DOI", "valueOf_": "doi:10.1038/s41586-026-11016-2"}
        {"type_": "PUBMED", "valueOf_": "42778700"}
    (other types such as ISSN or CSD are ignored). The plain `doi` / `pubmed`
    keys are checked first.
    """
    doi = strip_doi(text(cit.get("doi")))
    pubmed = text(cit.get("pubmed"))
    for ref in as_list(cit.get("external_references")):
        if isinstance(ref, dict):
            ref_type = str(ref.get("type_") or ref.get("type") or "").upper()
            if ref_type == "DOI":
                doi = doi or strip_doi(text(ref))
            elif ref_type.startswith("PUBMED"):
                pubmed = pubmed or text(ref)
    return doi, pubmed



def _subjects(entry: dict[str, Any]) -> tuple[list[str], str | None]:
    """
    Return (subjects, long_keywords).

    Subjects are the comma-separated parts of `admin.keywords` plus the structure
    determination methods (e.g. "singleParticle"). `admin.keywords` is free text:
    one entry has a single 200-character run of words followed by ", OXIDOREDUCTASE".
    Parts longer than MAX_SUBJECT_LENGTH are not used as subjects; instead the full
    keywords text is returned as `long_keywords` so it can be kept as a description.
    `long_keywords` is None when every part was short enough.
    """
    keywords = set[str]()
    keywords_text = text((entry.get("admin") or {}).get("keywords")) or ""
    has_long_keywords = False

    for part in keywords_text.split(","):
        part = part.strip()
        if not part:
            continue
        if len(part) <= MAX_SUBJECT_LENGTH:
            keywords.add(part)
        else:
            has_long_keywords = True

    determinations = (entry.get("structure_determination_list") or {}).get("structure_determination")
    for sd in as_list(determinations):
        method = text(sd.get("method")) if isinstance(sd, dict) else None
        if method:
            keywords.add(method)

    return sorted(keywords), (keywords_text if has_long_keywords else None)



def _descriptions(entry: dict[str, Any], long_keywords: str | None) -> list[Description]:
    """Abstract (details), the sample name, and the keywords text that was too long for subjects."""
    admin = entry.get("admin") or {}
    descriptions = []

    details = text(entry.get("details")) or text(admin.get("details"))
    if details:
        descriptions.append(Description(details, "Abstract"))

    sample_name = text((entry.get("sample") or {}).get("name"))
    if sample_name:
        descriptions.append(Description(f"Sample: {sample_name}", "Other"))

    if long_keywords:
        descriptions.append(Description(f"Keywords: {long_keywords}", "Other"))

    return descriptions



def _funding(entry: dict[str, Any]) -> list[FundingReference]:
    """Grants (`admin.grant_support`): funding body and, when given, the grant code."""
    grants = as_list(((entry.get("admin") or {}).get("grant_support") or {}).get("grant_reference"))
    return [
        FundingReference(funder_name = body, award_number = text(g.get("code")))
        for g in grants
        if isinstance(g, dict) and (body := text(g.get("funding_body")))
    ]



# ---------------------------------------------------------------------------
# File metadata
# ---------------------------------------------------------------------------

def emdb_files(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """
    The files named in the entry JSON, with the metadata the JSON gives for each:
    `file`, `role`, `format`, `size_kbytes`, `annotation_details`. Nothing is added.
    """
    found: dict[str, dict[str, Any]] = {}
    _collect_files(entry, "entry", found)

    # Primary map first (the JSON keys are alphabetical, so it would otherwise
    # come after the half maps); the sort is stable for everything else.
    return sorted(found.values(), key = lambda f: f["role"] != "map")



def _collect_files(node: Any, role: str, out: dict[str, dict[str, Any]]) -> None:
    """
    Walk the entry JSON and collect every object that has a `file` name (map,
    half maps, additional maps, masks, ...) into `out`, keyed by file name.
    `role` is the name of the JSON key the file sits under, e.g. "map" or "half_map".
    Fields the JSON does not give for a file are left out.
    """
    if isinstance(node, dict):
        name = text(node.get("file"))

        if name and name not in out:
            metadata: dict[str, Any] = {
                "file": name,
                "role": role,
                "format": text(node.get("format")),
                "size_kbytes": node.get("size_kbytes"),
                "annotation_details": text(node.get("annotation_details")),
            }
            out[name] = {key: value for key, value in metadata.items() if value is not None}

        for key, child in node.items():
            _collect_files(child, key if isinstance(child, (dict, list)) else role, out)

    elif isinstance(node, list):
        for child in node:
            _collect_files(child, role, out)