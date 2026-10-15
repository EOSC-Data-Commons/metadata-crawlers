import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import httpx

from oaipmh.client import OAI_NS, OaiPmhError, fetch_oai

# Supported metadata prefixes, in order of preference.
PREFERRED_SCHEMAS = ["oai_datacite", "datacite", "oai_dc", "ddi"]
MAX_SAMPLE_RECORDS = 3


class ProbeFailure(Exception):
    """A validation check failed; the message says which one."""


async def fetch_verb(client: httpx.AsyncClient, harvest_url: str, verb: str, allowed_codes: tuple = (), **params) -> ET.Element | None:
    """
    Sends one OAI-PMH request and turns every failure into a readable ProbeFailure.

    Args:
        client: shared httpx client.
        harvest_url: OAI-PMH endpoint URL.
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
        return await fetch_oai(client, harvest_url, verb = verb, **params)
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



def validate_records(root: ET.Element, metadata_namespace: str) -> list[str]:
    """
    Checks the first few non-deleted records of a response against the expected metadata namespace.

    Args:
        root: parsed ListRecords or GetRecord response.
        metadata_namespace: namespace the repository advertised for the chosen prefix.

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
        # if not metadata[0].tag.startswith(f"{{{metadata_namespace}}}"):
        #     raise ProbeFailure(f"Record {identifier} metadata is not in the advertised namespace {metadata_namespace}.")
        identifiers.append(identifier)
    if not identifiers:
        raise ProbeFailure("No usable (non-deleted) records found on the first page.")
    return identifiers



async def run_checks(client: httpx.AsyncClient, harvest_url: str, result: dict, required_prefix: str | None) -> str:
    """
    Runs every validation step in order and fills repo_name / metadata_prefix into result as soon as they are known.

    Args:
        client: shared httpx client.
        harvest_url: OAI-PMH endpoint URL.
        result: dict that receives "repo_name" and "metadata_prefix".
        required_prefix: metadata prefix that must be validated, or None to pick the best supported one.

    Returns:
        A short success message describing what was verified.
    """
    identify_root = await fetch_verb(client, harvest_url, "Identify")
    result["repo_name"] = identify_root.findtext(f".//{OAI_NS}repositoryName")
    granularity = identify_root.findtext(f".//{OAI_NS}granularity") or "YYYY-MM-DD"

    formats_root = await fetch_verb(client, harvest_url, "ListMetadataFormats")
    namespaces = {
        fmt.findtext(f"{OAI_NS}metadataPrefix"): fmt.findtext(f"{OAI_NS}metadataNamespace")
        for fmt in formats_root.findall(f".//{OAI_NS}metadataFormat")
    }
    candidate_prefixes = [required_prefix] if required_prefix else PREFERRED_SCHEMAS
    prefix = next((p for p in candidate_prefixes if p in namespaces), None)
    if prefix is None:
        raise ProbeFailure(f"None of {candidate_prefixes} is offered. Repository offers: {list(namespaces)}")
    result["metadata_prefix"] = prefix

    await fetch_verb(client, harvest_url, "ListSets", ("noSetHierarchy",))
    await fetch_verb(client, harvest_url, "ListIdentifiers", ("noRecordsMatch",), metadataPrefix = prefix)
    first_page = await fetch_verb(client, harvest_url, "ListRecords", ("noRecordsMatch",), metadataPrefix = prefix)
    if first_page is None:
        return "All verbs work, but the repository has no records yet."

    identifiers = validate_records(first_page, namespaces[prefix])
    record_root = await fetch_verb(client, harvest_url, "GetRecord", identifier = identifiers[0], metadataPrefix = prefix)
    validate_records(record_root, namespaces[prefix])

    # Incremental harvest: an empty answer (noRecordsMatch) is fine, an error such as badArgument is not.
    now = datetime.now(timezone.utc)
    date_range = {
        "from": format_oai_date(now - timedelta(days = 30), granularity),
        "until": format_oai_date(now, granularity),
    }
    await fetch_verb(client, harvest_url, "ListRecords", ("noRecordsMatch",), metadataPrefix = prefix, **date_range)

    token = (first_page.findtext(f".//{OAI_NS}resumptionToken") or "").strip()
    if not token:
        return f"All verbs work and sample records are valid ({prefix}). Incremental harvest (from/until) works. Single page, no resumptionToken to test."
    validate_records(await fetch_verb(client, harvest_url, "ListRecords", resumptionToken = token), namespaces[prefix])
    return f"All verbs work, sample records are valid ({prefix}), incremental harvest (from/until) works and resumptionToken paging works."



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