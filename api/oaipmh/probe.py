"""
Validates a candidate OAI-PMH repository: all six verbs, sample records in
the chosen metadata format, incremental harvesting (from/until) and paging
with a resumptionToken.

Sample records are validated against the format family of the selected
metadata prefix (see oaipmh/formats.py) rather than the exact namespace the
repository advertises: schema versions do not matter, but records served in
a different format than the selected one (e.g. Dublin Core records under a
"datacite" prefix) are rejected.
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import httpx

from oaipmh.client import OAI_NS, OaiPmhError, fetch_oai
from oaipmh.formats import (
    NAMESPACE_FAMILIES, PREFERRED_PREFIXES, SUPPORTED_PREFIXES, describe_family, is_accepted_namespace,
    is_supported_prefix,
)

MAX_SAMPLE_RECORDS = 3
INCREMENTAL_WINDOW_DAYS = 30
# Safety net for repositories that keep returning a resumptionToken on ListSets.
MAX_SET_PAGES = 100


class ProbeFailure(Exception):
    """A validation check failed; the message says which one."""


async def fetch_verb(client: httpx.AsyncClient, base_url: str, verb: str, allowed_codes: tuple = (), **params) -> ET.Element | None:
    """
    Sends one OAI-PMH request and turns every failure into a readable ProbeFailure.

    Args:
        client: shared httpx client.
        base_url: OAI-PMH endpoint URL.
        verb: OAI-PMH verb, e.g. "ListRecords".
        allowed_codes: OAI-PMH error codes that count as success (e.g. "noRecordsMatch").
        params: extra query parameters (metadataPrefix, identifier, resumptionToken...).

    Returns:
        Parsed XML root, or None if the repository answered with one of allowed_codes.
    """
    label = verb
    if "resumptionToken" in params:
        label += " with resumptionToken"
    elif "from" in params:
        label += " with from/until"
    try:
        return await fetch_oai(client, base_url, verb = verb, **params)
    except OaiPmhError as exc:
        if exc.code in allowed_codes:
            return None
        raise ProbeFailure(f"{label} returned OAI-PMH error {exc.code}: {exc.message}") from exc
    except (httpx.HTTPError, ET.ParseError) as exc:
        raise ProbeFailure(f"{label} failed: {type(exc).__name__}: {exc}") from exc


def format_oai_date(moment: datetime, granularity: str) -> str:
    """
    Formats a date the way the repository's Identify granularity requires.

    Args:
        moment: the date and time to format.
        granularity: the repository's granularity, "YYYY-MM-DD" or "YYYY-MM-DDThh:mm:ssZ".

    Returns:
        The date as a string usable in the from/until arguments.
    """
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ" if "T" in granularity else "%Y-%m-%d")


def element_namespaces(element: ET.Element) -> set[str]:
    """
    Collects the XML namespaces used anywhere inside an element and its descendants.

    Args:
        element: the element to inspect, e.g. the first child of a <metadata> element.

    Returns:
        Set of namespace URIs; namespace-less elements are ignored.
    """
    return {
        tag[1:].partition("}")[0]
        for tag in (descendant.tag for descendant in element.iter())
        if tag.startswith("{")
    }


def check_record_format(metadata: ET.Element, identifier: str, metadata_prefix: str) -> None:
    """
    Ensures a record's metadata belongs to the format family of the selected prefix.

    Any schema version of the family is accepted (e.g. every DataCite kernel version and the
    DataCite OAI-PMH profile), so the check is loose about versions but strict about the format
    itself: a record served in Dublin Core when "datacite" was selected fails here, even when the
    repository advertises the datacite prefix.

    Args:
        metadata: the record's <metadata> element.
        identifier: the record's OAI identifier, used in the failure message.
        metadata_prefix: the selected metadata prefix.

    Raises:
        ProbeFailure when the record is not in the selected format.
    """
    family = SUPPORTED_PREFIXES.get(metadata_prefix)
    if family is None:
        raise ProbeFailure(
            f"Cannot validate records for unsupported metadata prefix '{metadata_prefix}'. We cover: {list(SUPPORTED_PREFIXES)}."
        )
    if len(metadata) == 0:
        raise ProbeFailure(f"Record {identifier} has no <metadata> content.")
    namespaces = element_namespaces(metadata[0])
    if any(is_accepted_namespace(namespace, metadata_prefix) for namespace in namespaces):
        return
    root_namespace = metadata[0].tag[1:].partition("}")[0] if metadata[0].tag.startswith("{") else "no namespace"
    raise ProbeFailure(
        f"Record {identifier} metadata is in {describe_family(root_namespace) or root_namespace} format "
        f"({root_namespace}), but this endpoint uses metadata prefix '{metadata_prefix}' ({family}); "
        f"expected a {family} namespace starting with one of {NAMESPACE_FAMILIES[family]}."
    )


def validate_records(root: ET.Element, metadata_prefix: str, advertised_namespace: str | None = None) -> list[str]:
    """
    Checks the first few non-deleted records of a response against the selected metadata prefix.

    Prefixes we cover (see oaipmh/formats.py) are checked by format family: versions do not
    matter, but the records must really be in the selected format. Prefixes outside our supported
    list fall back to the old strict check against the advertised namespace.

    Args:
        root: parsed ListRecords or GetRecord response.
        metadata_prefix: the selected metadata prefix.
        advertised_namespace: namespace the repository advertised for the prefix (fallback only).

    Returns:
        Identifiers of the records that were checked.
    """
    identifiers = []
    for record in root.findall(f".//{OAI_NS}record")[:MAX_SAMPLE_RECORDS]:
        header = record.find(f"{OAI_NS}header")
        if header is None or header.get("status") == "deleted":
            continue
        identifier = (header.findtext(f"{OAI_NS}identifier") or "").strip()
        metadata = record.find(f"{OAI_NS}metadata")
        if not identifier or metadata is None or len(metadata) == 0:
            raise ProbeFailure("A sample record has no identifier or no <metadata> content.")
        if is_supported_prefix(metadata_prefix):
            check_record_format(metadata, identifier, metadata_prefix)
        elif advertised_namespace and not metadata[0].tag.startswith(f"{{{advertised_namespace}}}"):
            raise ProbeFailure(f"Record {identifier} metadata is not in the advertised namespace {advertised_namespace}.")
        identifiers.append(identifier)
    if not identifiers:
        raise ProbeFailure("No usable (non-deleted) records found on the first page.")
    return identifiers


async def run_checks(client: httpx.AsyncClient, base_url: str, result: dict, required_prefix: str | None) -> str:
    """
    Runs every validation step in order and fills repo_name / metadata_prefix into result as soon as they are known.

    Args:
        client: shared httpx client.
        base_url: OAI-PMH endpoint URL.
        result: dict that receives "repo_name" and "metadata_prefix".
        required_prefix: metadata prefix that must be validated, or None to pick the best supported one.

    Returns:
        A short success message describing what was verified.
    """
    identify_root = await fetch_verb(client, base_url, "Identify")
    result["repo_name"] = identify_root.findtext(f".//{OAI_NS}repositoryName")
    granularity = identify_root.findtext(f".//{OAI_NS}granularity") or "YYYY-MM-DD"

    formats_root = await fetch_verb(client, base_url, "ListMetadataFormats")
    namespaces = {
        fmt.findtext(f"{OAI_NS}metadataPrefix"): fmt.findtext(f"{OAI_NS}metadataNamespace")
        for fmt in formats_root.findall(f".//{OAI_NS}metadataFormat")
    }
    candidate_prefixes = [required_prefix] if required_prefix else PREFERRED_PREFIXES
    prefix = next((p for p in candidate_prefixes if p in namespaces), None)
    if prefix is None:
        raise ProbeFailure(f"None of {candidate_prefixes} is offered. Repository offers: {list(namespaces)}")
    result["metadata_prefix"] = prefix

    await fetch_verb(client, base_url, "ListSets", ("noSetHierarchy",))
    await fetch_verb(client, base_url, "ListIdentifiers", ("noRecordsMatch",), metadataPrefix = prefix)
    first_page = await fetch_verb(client, base_url, "ListRecords", ("noRecordsMatch",), metadataPrefix = prefix)
    if first_page is None:
        return "All verbs work, but the repository has no records yet."

    identifiers = validate_records(first_page, prefix, namespaces.get(prefix))
    record_root = await fetch_verb(client, base_url, "GetRecord", identifier = identifiers[0], metadataPrefix = prefix)
    validate_records(record_root, prefix, namespaces.get(prefix))

    # Incremental harvest: an empty answer (noRecordsMatch) is fine, an error such as badArgument is not.
    now = datetime.now(timezone.utc)
    date_range = {
        "from": format_oai_date(now - timedelta(days = INCREMENTAL_WINDOW_DAYS), granularity),
        "until": format_oai_date(now, granularity),
    }
    await fetch_verb(client, base_url, "ListRecords", ("noRecordsMatch",), metadataPrefix = prefix, **date_range)

    token = (first_page.findtext(f".//{OAI_NS}resumptionToken") or "").strip()
    if not token:
        return f"All verbs work and sample records are valid ({prefix}). Incremental harvest (from/until) works. Single page, no resumptionToken to test."
    validate_records(await fetch_verb(client, base_url, "ListRecords", resumptionToken = token), prefix, namespaces.get(prefix))
    return f"All verbs work, sample records are valid ({prefix}), incremental harvest (from/until) works and resumptionToken paging works."


async def fetch_repository_formats(client: httpx.AsyncClient, base_url: str) -> dict:
    """
    Runs only Identify and ListMetadataFormats, so submitters can see which metadata prefixes a
    repository offers before selecting one for submission.

    Args:
        client: shared httpx client.
        base_url: OAI-PMH endpoint URL.

    Returns:
        dict with "repo_name" and "offered_formats" (list of dicts with "metadata_prefix" and
        "metadata_namespace").
    """
    identify_root = await fetch_verb(client, base_url, "Identify")
    formats_root = await fetch_verb(client, base_url, "ListMetadataFormats")
    return {
        "repo_name": identify_root.findtext(f".//{OAI_NS}repositoryName"),
        "offered_formats": [
            {
                "metadata_prefix": fmt.findtext(f"{OAI_NS}metadataPrefix"),
                "metadata_namespace": fmt.findtext(f"{OAI_NS}metadataNamespace"),
            }
            for fmt in formats_root.findall(f".//{OAI_NS}metadataFormat")
        ],
    }


async def fetch_repository_sets(client: httpx.AsyncClient, base_url: str) -> list[dict]:
    """
    Lists the sets a repository offers (ListSets), so submitters can pick the ones to harvest.

    Follows resumption tokens so large set lists are complete, up to MAX_SET_PAGES pages.

    Args:
        client: shared httpx client.
        base_url: OAI-PMH endpoint URL.

    Returns:
        list of dicts with "set_spec" and "set_name"; empty when the repository has no sets
        (noSetHierarchy).

    Raises:
        ProbeFailure: when ListSets fails.
    """
    sets: list[dict] = []
    token = ""
    for _ in range(MAX_SET_PAGES):
        if token:
            page = await fetch_verb(client, base_url, "ListSets", ("noSetHierarchy",), resumptionToken = token)
        else:
            page = await fetch_verb(client, base_url, "ListSets", ("noSetHierarchy",))
        if page is None:  # noSetHierarchy: the repository has no sets at all
            return []
        sets.extend(
            {"set_spec": s.findtext(f"{OAI_NS}setSpec"), "set_name": s.findtext(f"{OAI_NS}setName")}
            for s in page.findall(f".//{OAI_NS}set")
        )
        token = (page.findtext(f".//{OAI_NS}resumptionToken") or "").strip()
        if not token:  # a missing or empty resumptionToken marks the last page
            return sets
    return sets


async def inspect_repository(base_url: str) -> dict:
    """
    Connects to a repository and reports its name, the metadata formats it offers and its sets.

    Args:
        base_url: OAI-PMH endpoint URL.

    Returns:
        dict with "repo_name", "offered_formats" (metadata_prefix, metadata_namespace) and
        "offered_sets" (set_spec, set_name); "offered_sets" is empty when the repository has no
        sets or does not answer ListSets.

    Raises:
        ProbeFailure: when the repository cannot be reached or does not answer Identify or
            ListMetadataFormats.
    """
    async with httpx.AsyncClient(timeout = 15, follow_redirects = True) as client:
        info = await fetch_repository_formats(client, base_url)
        try:
            info["offered_sets"] = await fetch_repository_sets(client, base_url)
        except ProbeFailure:
            # Sets are optional information for submitters: a repository whose ListSets fails
            # here still shows its formats (the full probe rejects it later anyway).
            info["offered_sets"] = []
        return info


async def list_repository_sets(base_url: str) -> list[dict]:
    """
    Connects to a repository and lists its sets; used to check a submitter's set selection.

    Args:
        base_url: OAI-PMH endpoint URL.

    Returns:
        list of dicts with "set_spec" and "set_name"; empty when the repository has no sets.

    Raises:
        ProbeFailure: when the repository cannot be reached or does not answer ListSets.
    """
    async with httpx.AsyncClient(timeout = 15, follow_redirects = True) as client:
        return await fetch_repository_sets(client, base_url)


async def probe_oaipmh(base_url: str, required_prefix: str | None = None) -> dict:
    """
    Validates a repository end to end.

    Args:
        base_url: OAI-PMH endpoint URL.
        required_prefix: metadata prefix to validate (use the configured one when re-checking), or None to auto-detect.

    Returns:
        dict with "repo_name", "metadata_prefix" (None if undetected), "passed" (bool) and "message" (success note or failure reason).
    """
    result = {"repo_name": None, "metadata_prefix": None, "passed": False, "message": ""}
    try:
        async with httpx.AsyncClient(timeout = 15, follow_redirects = True) as client:
            result["message"] = await run_checks(client, base_url, result, required_prefix)
            result["passed"] = True
    except ProbeFailure as failure:
        result["message"] = str(failure)
    return result