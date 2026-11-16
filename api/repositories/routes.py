"""HTTP routes for managing repositories and for submitting, validating, listing and approving OAI-PMH endpoints."""

from typing import Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import HttpUrl

from auth import get_current_user, require_role
from oaipmh.formats import SUPPORTED_PREFIXES, is_supported_prefix
from oaipmh.probe import ProbeFailure, inspect_repository, list_repository_sets, probe_oaipmh
from warehouse import get_client
from repositories.crud import (
    count_endpoints, find_by_harvest_url, find_repository_id, get_endpoint, insert_endpoint,
    insert_repository, list_endpoints, list_repositories, normalize_url, update_validation_result,
)

# Every route requires a logged-in user; individual routes add role checks.
router = APIRouter()  # LOCAL TEST: auth disabled - restore: dependencies = [Depends(get_current_user)]


def is_active_after_validation(passed: bool, current_is_active: bool | None = None) -> bool | None:
    """
    Decides the is_active value to store after a validation run.

    Args:
        passed: whether validation succeeded.
        current_is_active: value before this run (None for new endpoints).

    Returns:
        False if failed; True if it was already approved; otherwise None (waiting for review).
    """
    return (current_is_active or None) if passed else False


@router.post("/repositories", status_code = 201, tags = ["Repositories"])  # LOCAL TEST: auth disabled - restore: dependencies = [Depends(require_role("admin", "submitter"))]
async def create_repository(
    code: str,
    name: str,
    description: str | None = None,
    base_url: HttpUrl | None = None,
    client: httpx.AsyncClient = Depends(get_client),
):
    """
    Creates a new repository that endpoints can then be attached to.

    Args:
        code: unique short code.
        name: human-readable repository name.
        description: free-text description of the repository.
        base_url: URL of the repository's own website (its home page, NOT the OAI-PMH endpoint).
        client: warehouse API client.

    Returns:
        The inserted repository row.
    """
    if await find_repository_id(client, code) is not None:
        raise HTTPException(status_code = 409, detail = f"Repository code '{code}' already exists")
    return await insert_repository(client, code, name, description, str(base_url) if base_url else None)


@router.get("/repositories", tags = ["Repositories"])  # LOCAL TEST: auth disabled - restore: dependencies = [Depends(require_role("admin", "submitter"))]
async def show_repositories(client: httpx.AsyncClient = Depends(get_client)):
    """
    Lists all repositories, so you can see which codes are valid for new endpoints.

    Args:
        client: warehouse API client.

    Returns:
        List of repository rows.
    """
    return await list_repositories(client)


@router.get("/endpoints/metadata-formats", tags = ["Metadata prefixes"])  # LOCAL TEST: auth disabled - restore: dependencies = [Depends(require_role("admin", "submitter"))]
async def show_metadata_formats(base_url: HttpUrl):
    """
    Shows the metadata formats and sets a repository offers, so submitters can choose a
    metadata_prefix (and the sets to harvest) before submitting an endpoint with POST /endpoints.

    Every offered format is marked as supported or not. We recommend the supported prefixes in
    the order of "supported_prefixes" (DataCite first: oai_datacite, oai_datacite4, datacite,
    then Dublin Core, then DDI), but the choice is the submitter's: pass it as metadata_prefix
    to POST /endpoints. Schema versions do not matter within a family.

    Args:
        base_url: OAI-PMH base URL of the candidate endpoint.

    Returns:
        dict with "repo_name", "supported_prefixes" (prefix -> format family, in preference
        order), "offered_formats" (metadata_prefix, metadata_namespace, supported flag) and
        "offered_sets" (set_spec, set_name; empty when the repository has no sets).
    """
    try:
        info = await inspect_repository(normalize_url(str(base_url)))
    except ProbeFailure as failure:
        raise HTTPException(status_code = 502, detail = f"Repository check failed: {failure}")

    offered_formats = [
        {**fmt, "supported": is_supported_prefix(fmt["metadata_prefix"])}
        for fmt in info["offered_formats"]
    ]
    return {
        "repo_name": info["repo_name"],
        "supported_prefixes": SUPPORTED_PREFIXES,
        "offered_formats": offered_formats,
        "offered_sets": info["offered_sets"],
    }


@router.post("/endpoints", status_code = 201, tags = ["Endpoint submission"])  # LOCAL TEST: auth disabled - restore: dependencies = [Depends(require_role("admin", "submitter"))]
async def submit_endpoint(
    repository_code: str,
    base_url: HttpUrl,
    name: str | None = None,
    scientific_discipline: str = "Multidisciplinary",
    metadata_prefix: str | None = None,
    set_: list[str] | None = Query(default = None, alias = "set"),
    harvest_interval_days: int = 7,
    client: httpx.AsyncClient = Depends(get_client),
):
    """
    Validates a new OAI-PMH endpoint and inserts it into endpoints (is_active NULL if valid, FALSE if not).

    Args:
        repository_code: code of an existing repository (see GET /repositories).
        base_url: OAI-PMH base URL.
        name: unique endpoint name; defaults to the name returned by Identify.
        scientific_discipline: discipline label.
        metadata_prefix: the metadata format to harvest, selected by the submitter. Call GET
            /endpoints/metadata-formats first to see which formats the repository offers and which
            of them we support. Must be one of oai_datacite, oai_datacite4, datacite, oai_dc,
            oai_ddi25; omit to auto-detect the best supported one. Sample records must really be
            in the selected format (any schema version is fine, e.g. any DataCite kernel version).
        set_: OAI-PMH sets to harvest, selected by the submitter (repeat the parameter for
            several sets, e.g. ?set=entityType:Record&set=entityType:Dataset). Call GET
            /endpoints/metadata-formats first to see the repository's sets; omit to harvest the
            whole repository.
        harvest_interval_days: harvest schedule in days.
        client: warehouse API client.

    Returns:
        The inserted endpoint row.
    """
    if metadata_prefix is not None and not is_supported_prefix(metadata_prefix):
        raise HTTPException(
            status_code = 422,
            detail = f"Unsupported metadata prefix '{metadata_prefix}'. We cover: {list(SUPPORTED_PREFIXES)}.",
        )

    harvest_url = normalize_url(str(base_url))
    repository_id = await find_repository_id(client, repository_code)
    if repository_id is None:
        raise HTTPException(
            status_code = 404,
            detail = f"Unknown repository code '{repository_code}'. Create it first with POST /repositories.",
        )
    if await find_by_harvest_url(client, harvest_url) is not None:
        raise HTTPException(status_code = 409, detail = "This endpoint already exists")

    # Sets are optional: they are validated against the repository's own sets and stored only
    # when the submitter picked at least one (otherwise the whole repository is harvested).
    selected_sets = [s for s in (set_ or []) if s.strip()] or None
    if selected_sets:
        try:
            offered_specs = {s["set_spec"] for s in await list_repository_sets(harvest_url)}
        except ProbeFailure as failure:
            raise HTTPException(status_code = 502, detail = f"Repository check failed: {failure}")
        unknown_sets = [s for s in selected_sets if s not in offered_specs]
        if unknown_sets:
            raise HTTPException(
                status_code = 422,
                detail = (
                    f"Unknown set(s) {unknown_sets}. "
                    f"This repository offers: {sorted(offered_specs) if offered_specs else 'no sets'}."
                ),
            )

    result = await probe_oaipmh(harvest_url, metadata_prefix)
    try:
        return await insert_endpoint(
            client, repository_id, name or result["repo_name"] or harvest_url, harvest_url,
            scientific_discipline, is_active_after_validation(result["passed"]),
            result["metadata_prefix"], selected_sets, harvest_interval_days, result["message"],
        )
    except httpx.HTTPStatusError as error:
        # e.g. 409 from the warehouse when the endpoint name already exists
        raise HTTPException(status_code = error.response.status_code, detail = error.response.text)


@router.post("/endpoints/validate", tags = ["Endpoint validation"])  # LOCAL TEST: auth disabled - restore: dependencies = [Depends(require_role("admin"))]
async def validate_endpoint(base_url: HttpUrl, client: httpx.AsyncClient = Depends(get_client)):
    """
    Re-runs validation on an existing endpoint, found by its URL, using its configured metadata prefix.

    Sample records are checked against the format family of that prefix (any schema version is
    fine, but e.g. Dublin Core records under a datacite prefix fail validation).

    Args:
        base_url: OAI-PMH base URL of an endpoint already in the table.
        client: warehouse API client.

    Returns:
        The updated endpoint row.
    """
    endpoint = await find_by_harvest_url(client, normalize_url(str(base_url)))
    if endpoint is None:
        raise HTTPException(status_code = 404, detail = "No endpoint with this URL. Add it with POST /endpoints.")

    result = await probe_oaipmh(endpoint["harvest_url"], endpoint["metadata_prefix"])
    return await update_validation_result(
        client, endpoint["id"], is_active_after_validation(result["passed"], endpoint["is_active"]),
        result["metadata_prefix"], result["message"],
    )


@router.get("/endpoints/summary", tags = ["Endpoint review"])  # LOCAL TEST: auth disabled - restore: dependencies = [Depends(require_role("admin"))]
async def endpoints_summary(client: httpx.AsyncClient = Depends(get_client)):
    """
    Counts endpoints per state.

    Args:
        client: warehouse API client.

    Returns:
        dict with waiting, approved, failed and total counts.
    """
    return await count_endpoints(client)


@router.get("/endpoints", tags = ["Endpoint review"])  # LOCAL TEST: auth disabled - restore: dependencies = [Depends(require_role("admin"))]
async def show_endpoints(
    state: Literal["all", "waiting", "approved", "failed"] = "all",
    client: httpx.AsyncClient = Depends(get_client),
):
    """
    Lists endpoints, optionally only the waiting, approved or failed ones.

    Args:
        state: which endpoints to list (default "all").
        client: warehouse API client.

    Returns:
        List of endpoint rows, newest first.
    """
    return await list_endpoints(client, state)


@router.post("/endpoints/{endpoint_id}/approve", tags = ["Endpoint review"])
async def approve_endpoint(
    endpoint_id: str,  # warehouse endpoint ids are UUIDs, not integers
    admin: dict = {"username": "local-dev"},  # LOCAL TEST: auth disabled - restore: Depends(require_role("admin"))
    client: httpx.AsyncClient = Depends(get_client),
):
    """
    Approves a waiting endpoint (sets is_active to TRUE) so the pipeline can harvest it.

    Args:
        endpoint_id: id of the endpoint to approve.
        admin: the logged-in admin.
        client: warehouse API client.

    Returns:
        The updated endpoint row.
    """
    endpoint = await get_endpoint(client, endpoint_id)
    if endpoint is None:
        raise HTTPException(status_code = 404, detail = "Endpoint not found")
    if endpoint["is_active"] is not None:
        raise HTTPException(status_code = 409, detail = "Only waiting endpoints can be approved")

    return await update_validation_result(
        client, endpoint_id, True, None, f"Approved by {admin['username']}. {endpoint['description']}",
    )