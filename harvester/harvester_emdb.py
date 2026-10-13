import logging, re, time
from typing import Any, Iterator, cast
from datetime import datetime

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
    get_json,
    make_client,
    run_harvester,
)

_EMDB_CLIENT = make_client(120)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://www.ebi.ac.uk"
SEARCH_PATH = "/emdb/api/search/{query}"
ENTRY_PATH = "/emdb/api/entry/{emdb_id}"
FTP_BASE = "https://ftp.ebi.ac.uk/pub/databases/emdb/structures/{emdb_id}/"
PAGE_SIZE = 100
PAGE_DELAY_SECONDS = 2
SEARCH_FIELDS = "*"
INCREMENTAL_DATE_FIELD = "update_date"
DELETED_STATUS_CODES = {"OBS"}
MAX_SUBJECT_LENGTH = 100
EMDB_ID_RE = re.compile(r"^EMD-\d+$")



def _val(x: Any) -> Any:
    """Unwrap `{"valueOf_": "text", ...}` style values; first item of a list."""
    if isinstance(x, dict):
        for key in ("valueOf_", "#text", "value"):
            if key in x:
                return _val(x[key])
        return None
    if isinstance(x, list):
        return _val(x[0]) if x else None
    return x



def _s(x: Any) -> str | None:
    """`_val` as a stripped, non-empty string, or None."""
    v = _val(x)
    if v is None:
        return None
    v = str(v).strip()
    return v or None



def _as_list(x: Any) -> list[Any]:
    if x is None:
        return []
    return x if isinstance(x, list) else [x]



def _strip_doi(doi: str | None) -> str | None:
    if not doi:
        return None
    return doi.split("doi:")[-1] if doi.lower().startswith("doi:") else doi



def _solr_date(value: str) -> str:
    """
    Convert a run timestamp to the format the EMDB Solr range query accepts.
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%dT%H:%M:%SZ")



def search_page(page: int, query: str) -> Any:
    """
    Fetch one page from `GET /emdb/api/search/{query}`.

    :param page: The page number to fetch (1-based).
    :param query: The Solr query.
    :return: The parsed JSON payload for that page.
    """
    url = DEFAULT_BASE_URL + SEARCH_PATH.format(query = query)
    params: dict[str, Any] = {"rows": PAGE_SIZE, "page": page, "fl": SEARCH_FIELDS}
    return get_json(_EMDB_CLIENT, url, params = params)



def _doc_id(doc: Any) -> str | None:
    """Find an EMD-xxxx id in one search hit (a string or a dict)."""
    if isinstance(doc, str):
        return doc if EMDB_ID_RE.match(doc) else None
    if isinstance(doc, dict):
        for key in ("emdb_id", "id", "entry_id"):
            v = doc.get(key)
            if isinstance(v, str) and EMDB_ID_RE.match(v):
                return v
        for v in doc.values():
            if isinstance(v, str) and EMDB_ID_RE.match(v):
                return v
    return None



def extract_ids(payload: Any) -> list[str]:
    """
    An empty page returns `[]`. A non-empty payload we cannot read raises, so a
    wrong assumption fails loudly instead of "successfully" harvesting nothing.
    """
    if not payload:
        return []

    if isinstance(payload, dict):
        keyed = [k for k in payload if isinstance(k, str) and EMDB_ID_RE.match(k)]
        if keyed:
            return keyed
        for key in ("results", "entries", "docs", "response", "data", "items"):
            if key in payload:
                return extract_ids(payload[key])
        raise ValueError(f"Unrecognised EMDB search payload, keys: {list(payload)[:10]}")

    if isinstance(payload, list):
        ids = [i for i in map(_doc_id, payload) if i]
        if not ids:
            raise ValueError(f"Could not find EMDB ids in search hits, first hit: {str(payload[0])[:200]}")
        return ids

    raise ValueError(f"Unrecognised EMDB search payload type: {type(payload).__name__}")



def _search_ids(query: str) -> Iterator[str]:
    """
    Walk every page for `query` and yield EMDB ids. Stops at the first empty
    page. If a page contains only ids we have already seen, pagination is not
    advancing (wrong paging parameters), so it raises instead of looping forever.
    """
    page = 1
    seen = set[str]()
    while True:
        ids = extract_ids(search_page(page, query))

        if not ids:
            logger.info("Page %d empty, stopping", page)
            break

        new_ids = [i for i in ids if i not in seen]
        if not new_ids:
            raise RuntimeError(f"Page {page} returned only already-seen ids: paging parameters are probably wrong")

        seen.update(new_ids)
        logger.info("Page %d: %d records (%d total so far)", page, len(new_ids), len(seen))
        yield from new_ids

        page += 1
        time.sleep(PAGE_DELAY_SECONDS)



def search_all() -> Iterator[str]:
    """Yield the id of every EMDB entry."""
    return _search_ids("*")



def search_incremental(from_date: str, until_date: str) -> Iterator[str]:
    """
    Yield the ids of entries whose `INCREMENTAL_DATE_FIELD` falls in the range.

    :param from_date: Start of the range (YYYY-MM-DD or a full timestamp).
    :param until_date: End of the range.
    """
    return _search_ids(
        f"{INCREMENTAL_DATE_FIELD}:[{_solr_date(from_date)} TO {_solr_date(until_date)}]"
    )



def fetch_entry(emdb_id: str) -> dict[str, Any]:
    """Fetch the full entry JSON for one EMDB id."""
    url = DEFAULT_BASE_URL + ENTRY_PATH.format(emdb_id = emdb_id)
    entry = get_json(_EMDB_CLIENT, url)
    if isinstance(entry, dict) and isinstance(entry.get(emdb_id), dict):
        entry = entry[emdb_id]
    return cast(dict[str, Any], entry)



def _collect_files(node: Any, role: str, out: dict[str, dict[str, Any]]) -> None:
    """
    Walk the entry JSON and collect every object that has a `file` name.
    """
    if isinstance(node, dict):
        name = _s(node.get("file"))

        if name and name not in out:
            metadata = {
                "name": name,
                "role": role,
                "format": _s(node.get("format")),
                "size_kbytes": node.get("size_kbytes"),
                "dimensions": node.get("dimensions"),
                "pixel_spacing": node.get("pixel_spacing"),
                "data_type": node.get("data_type"),
                "statistics": node.get("statistics"),
                "symmetry": node.get("symmetry"),
                "cell": node.get("cell"),
                "axis_order": node.get("axis_order"),
                "origin": node.get("origin"),
                "spacing": node.get("spacing"),
                "contour_list": node.get("contour_list"),
                "annotation_details": node.get("annotation_details"),
            }

            out[name] = {
                key: value
                for key, value in metadata.items()
                if value is not None
            }

        for key, child in node.items():
            _collect_files(
                child,
                key if isinstance(child, (dict, list)) else role,
                out,
            )

    elif isinstance(node, list):
        for child in node:
            _collect_files(child, role, out)



def emdb_files(emdb_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    """
    File metadata for one entry: the header XML plus every file named in the
    entry JSON. `ftp_base` is the entry's directory in the EMDB archive.
    """
    number = emdb_id.removeprefix("EMD-")
    ftp_base = FTP_BASE.format(emdb_id = emdb_id)

    found: dict[str, dict[str, Any]] = {}
    _collect_files(entry, "entry", found)

    files = [{
        "name": f"emd-{number}.xml",
        "role": "header",
        "format": "XML",
        "size_kbytes": None,
        "url": f"{ftp_base}header/emd-{number}.xml",
    }]
    # Primary map first (the JSON keys are alphabetical, so it would otherwise
    # come after the half maps); the sort is stable for everything else.
    files.extend(sorted(found.values(), key = lambda f: f["role"] != "map"))
    return {"ftp_base": ftp_base, "files": files}



def _people(authors: Any) -> list[Person]:
    people = []
    for a in _as_list(authors):
        name = _s(a)
        if name:
            orcid = _s(a.get("ORCID") or a.get("@ORCID")) if isinstance(a, dict) else None
            people.append(Person(name = name, orcid = orcid))
    return people



def _primary_citation(entry: dict[str, Any]) -> dict[str, Any]:
    cit = ((entry.get("crossreferences") or {}).get("citation_list") or {}).get("primary_citation") or {}
    cit_type = cit.get("citation_type") or {}
    return cit_type.get("journal_citation") or cit_type.get("non_journal_citation") or cit_type



def _citation_ids(cit: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return (doi, pubmed_id) of a citation. Checks the plain keys and the
    `external_references` list. The list items look like
    `{"type_": "CSD", "valueOf_": "0353"}` (verified); that the DOI / PubMed
    ones use the type values "DOI" / "PUBMED"."""
    doi = _strip_doi(_s(cit.get("doi")))
    pubmed = _s(cit.get("pubmed"))
    for ref in _as_list(cit.get("external_references")):
        if isinstance(ref, dict):
            ref_type = str(ref.get("type_") or ref.get("type") or "").upper()
            if ref_type == "DOI":
                doi = doi or _strip_doi(_s(ref))
            elif ref_type.startswith("PUBMED"):
                pubmed = pubmed or _s(ref)
    return doi, pubmed



def emdb_to_common(emdb_id: str, entry: dict[str, Any]) -> tuple[CommonRecord, str, bool]:
    """
    Map an EMDB entry JSON onto `CommonRecord`.

    :param emdb_id: e.g. "EMD-1234".
    :param entry: Entry dictionary from `/emdb/api/entry/{id}`.
    :return: (common record, datestamp YYYY-MM-DD, is_deleted).
    """
    admin = entry.get("admin") or {}
    key_dates = admin.get("key_dates") or {}

    deposition = _s(key_dates.get("deposition")) or ""
    release = _s(key_dates.get("map_release")) or _s(key_dates.get("header_release")) or ""
    update = _s(key_dates.get("update")) or ""

    # IDENTIFIER (mandatory)
    identifier, identifier_type = f"https://www.ebi.ac.uk/emdb/{emdb_id}", "URL"

    # CREATORS: entry authors, else the primary citation's authors
    citation = _primary_citation(entry)
    creators = _people((admin.get("authors_list") or {}).get("author")) or _people(citation.get("author"))

    # DATES
    dates = []
    if deposition:
        dates.append(DateEntry("Submitted", deposition[:10]))
    if release:
        dates.append(DateEntry("Available", release[:10]))
    if update:
        dates.append(DateEntry("Updated", update[:10]))

    # RELATED IDENTIFIERS
    related = []
    cit_doi, cit_pubmed = _citation_ids(citation)
    if cit_doi:
        related.append(RelatedIdentifier("DOI", "IsDocumentedBy", cit_doi))
    if cit_pubmed:
        related.append(RelatedIdentifier("URL", "IsDocumentedBy", f"https://pubmed.ncbi.nlm.nih.gov/{cit_pubmed}/"))


    emdb_list = (entry.get("crossreferences") or {}).get("emdb_list") or {}
    for ref in _as_list(emdb_list.get("emdb_reference")):
        related_id = _s(ref.get("emdb_id")) if isinstance(ref, dict) else _s(ref)
        if related_id and related_id != emdb_id:
            related.append(RelatedIdentifier("URL", "References", f"https://www.ebi.ac.uk/emdb/{related_id}"))

    pdb_list = (entry.get("crossreferences") or {}).get("pdb_list") or {}
    for ref in _as_list(pdb_list.get("pdb_reference")):
        pdb_id = _s(ref.get("pdb_id")) if isinstance(ref, dict) else _s(ref)
        if pdb_id:
            related.append(RelatedIdentifier("URL", "IsSourceOf", f"https://www.ebi.ac.uk/pdbe/entry/pdb/{pdb_id.lower()}"))

    # KEYWORDS
    keywords = set[str]()
    keywords_text = _s(admin.get("keywords")) or ""
    has_long_keywords = False
    for part in keywords_text.split(","):
        part = part.strip()
        if not part:
            continue
        if len(part) <= MAX_SUBJECT_LENGTH:
            keywords.add(part)
        else:
            has_long_keywords = True
    sd_list = (entry.get("structure_determination_list") or {}).get("structure_determination")
    for sd in _as_list(sd_list):
        method = _s(sd.get("method")) if isinstance(sd, dict) else None
        if method:
            keywords.add(method)

    # DESCRIPTIONS
    descriptions = []
    details = _s(entry.get("details")) or _s(admin.get("details"))
    if details:
        descriptions.append(Description(details, "Abstract"))
    sample_name = _s((entry.get("sample") or {}).get("name"))
    if sample_name:
        descriptions.append(Description(f"Sample: {sample_name}", "Other"))
    if has_long_keywords:
        descriptions.append(Description(f"Keywords: {keywords_text}", "Other"))

    # FUNDING REFERENCES
    funding = [
        FundingReference(funder_name = body, award_number = _s(g.get("code")))
        for g in _as_list((admin.get("grant_support") or {}).get("grant_reference"))
        if isinstance(g, dict) and (body := _s(g.get("funding_body")))
    ]

    status_code = _s((admin.get("current_status") or {}).get("code"))
    datestamp = (update or release or deposition)[:10]
    pub_year_source = release or deposition

    common = CommonRecord(
        identifier = identifier,
        identifier_type = identifier_type,
        title = _s(admin.get("title")),
        publisher = "EMBL-European Bioinformatics Institute",
        publication_year = pub_year_source[:4] if pub_year_source else None,
        resource_type = "Electron microscopy map",
        resource_type_general = "Dataset",
        creators = creators,
        dates = dates,
        related_identifiers = related,
        subjects = sorted(keywords),
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
    return common, datestamp, status_code in DELETED_STATUS_CODES



class EmdbAdapter(SourceAdapter):
    name = "EMDB"

    def iter_raw(self, from_date: str | None, until_date: str) -> Iterator[str]:
        """Yield EMDB ids."""
        if not from_date:
            yield from search_all()
        else:
            yield from search_incremental(from_date, until_date)

    def build_record(self, raw: str) -> HarvestedRecord:
        emdb_id = raw
        entry = fetch_entry(emdb_id)
        metadata, datestamp, is_deleted = emdb_to_common(emdb_id, entry)

        return HarvestedRecord(
            record_id = emdb_id,
            datestamp = datestamp,
            metadata = metadata,
            additional_metadata = emdb_files(emdb_id, entry),
            is_deleted = is_deleted,
        )



def run_harvester_emdb(run_info: dict[str, Any]) -> bool:
    """
    Run a full (or incremental) EMDB harvest and push each entry to the data
    warehouse as a harvest event.

    :param run_info: Dictionary describing the harvest run.
    :return: `True` if every harvest event was sent successfully, `False` otherwise.
    """
    return run_harvester(run_info, EmdbAdapter())