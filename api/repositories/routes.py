import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import HttpUrl
from typing import Literal

from auth import get_current_user, require_role
from db import get_pool
from oaipmh.probe import probe_oaipmh
from repositories.crud import (
    count_endpoints, find_by_harvest_url, find_repository_id, get_endpoint, insert_endpoint,
    insert_repository, list_endpoints, list_repositories, normalize_url, update_validation_result,
)

# Every route requires a logged-in user.
router = APIRouter(dependencies = [Depends(get_current_user)])



def is_active_after_validation(passed: bool, current_is_active: bool | None = None) -> bool | None:
    """
    Decides the is_active value to store after a validation run.

    Args:
        passed: whether validation succeeded.
        current_is_active: value before this run.

    Returns:
        False if failed; True if it was already approved; otherwise None (waiting for review).
    """
    return (current_is_active or None) if passed else False



@router.post("/repositories", status_code = 201, dependencies = [Depends(require_role("admin"))])
async def create_repository(code: str, name: str, pool: asyncpg.Pool = Depends(get_pool)):
    """
    Creates a new repository that endpoints can then be attached to.

    Args:
        code: unique short code.
        name: human-readable repository name.
        pool: database pool.

    Returns:
        The inserted repository row.
    """
    if await find_repository_id(pool, code) is not None:
        raise HTTPException(status_code = 409, detail = f"Repository code '{code}' already exists")
    return await insert_repository(pool, code, name)



@router.get("/repositories", dependencies = [Depends(require_role("admin", "submitter"))])
async def show_repositories(pool: asyncpg.Pool = Depends(get_pool)):
    """
    Lists all repositories, so you can see which codes are valid for new endpoints.

    Args:
        pool: database pool.

    Returns:
        List of repository rows.
    """
    return await list_repositories(pool)



@router.post("/endpoints", status_code = 201, dependencies = [Depends(require_role("admin", "submitter"))])
async def submit_endpoint(
    repository_code: str,
    base_url: HttpUrl,
    name: str | None = None,
    scientific_discipline: str = "Multidisciplinary",
    harvest_interval_days: int = 7,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """
    Validates a new OAI-PMH endpoint and inserts it into endpoints (is_active NULL if valid, FALSE if not).

    Args:
        repository_code: code of an existing repository (see GET /repositories).
        base_url: OAI-PMH base URL.
        name: unique endpoint name; defaults to the name returned by Identify.
        scientific_discipline: discipline label.
        harvest_interval_days: harvest schedule in days.
        pool: database pool.

    Returns:
        The inserted endpoint row.
    """
    harvest_url = normalize_url(str(base_url))
    repository_id = await find_repository_id(pool, repository_code)
    if repository_id is None:
        raise HTTPException(
            status_code = 404,
            detail = f"Unknown repository code '{repository_code}'. Create it first with POST /repositories.",
        )
    if await find_by_harvest_url(pool, harvest_url) is not None:
        raise HTTPException(status_code = 409, detail = "This endpoint already exists")

    result = await probe_oaipmh(harvest_url)
    try:
        return await insert_endpoint(
            pool, repository_id, name or result["repo_name"] or harvest_url, harvest_url,
            scientific_discipline, is_active_after_validation(result["passed"]),
            result["metadata_prefix"], harvest_interval_days, result["message"],
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code = 409, detail = "An endpoint with this name already exists")



@router.post("/endpoints/validate", dependencies = [Depends(require_role("admin"))])
async def validate_endpoint(base_url: HttpUrl, pool: asyncpg.Pool = Depends(get_pool)):
    """
    Re-runs validation on an existing endpoint, found by its URL, using its configured metadata prefix.

    Args:
        base_url: OAI-PMH base URL of an endpoint already in the table.
        pool: database pool.

    Returns:
        The updated endpoint row.
    """
    endpoint = await find_by_harvest_url(pool, normalize_url(str(base_url)))
    if endpoint is None:
        raise HTTPException(status_code = 404, detail = "No endpoint with this URL. Add it with POST /endpoints.")

    result = await probe_oaipmh(endpoint["harvest_url"], endpoint["metadata_prefix"])
    return await update_validation_result(
        pool, endpoint["id"], is_active_after_validation(result["passed"], endpoint["is_active"]),
        result["metadata_prefix"], result["message"],
    )



@router.get("/endpoints/summary", dependencies = [Depends(require_role("admin"))])
async def endpoints_summary(pool: asyncpg.Pool = Depends(get_pool)):
    """
    Counts endpoints per state.

    Args:
        pool: database pool.

    Returns:
        dict with waiting, approved, failed and total counts.
    """
    return await count_endpoints(pool)



@router.get("/endpoints", dependencies = [Depends(require_role("admin"))])
async def show_endpoints(
    state: Literal["all", "waiting", "approved", "failed"] = "all",
    pool: asyncpg.Pool = Depends(get_pool),
):
    """
    Lists endpoints, optionally only the waiting, approved or failed ones.

    Args:
        state: which endpoints to list (default "all").
        pool: database pool.

    Returns:
        List of endpoint rows, newest first.
    """
    return await list_endpoints(pool, state)



@router.post("/endpoints/{endpoint_id}/approve")
async def approve_endpoint(
    endpoint_id: int,
    admin: dict = Depends(require_role("admin")),
    pool: asyncpg.Pool = Depends(get_pool),
):
    """
    Approves a waiting endpoint (sets is_active to TRUE) so the pipeline can harvest it.

    Args:
        endpoint_id: id of the endpoint to approve.
        admin: the logged-in admin.
        pool: database pool.

    Returns:
        The updated endpoint row.
    """
    endpoint = await get_endpoint(pool, endpoint_id)
    if endpoint is None:
        raise HTTPException(status_code = 404, detail = "Endpoint not found")
    if endpoint["is_active"] is not None:
        raise HTTPException(status_code = 409, detail = "Only waiting endpoints can be approved")

    return await update_validation_result(
        pool, endpoint_id, True, None, f"Approved by {admin['username']}. {endpoint['description']}",
    )