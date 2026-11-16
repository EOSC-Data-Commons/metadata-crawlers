"""Access to endpoints and repositories through the warehouse API (no database, no credentials)."""

import httpx

# is_active: None = waiting for admin review, True = approved, False = failed.
STATE_FILTERS = {
    "all": lambda is_active: True,
    "waiting": lambda is_active: is_active is None,
    "approved": lambda is_active: is_active is True,
    "failed": lambda is_active: is_active is False,
}


def normalize_url(base_url: str) -> str:
    """
    Removes trailing slashes so equal URLs compare equal.

    Args:
        base_url: URL as entered by the user.

    Returns:
        The URL without trailing slashes.
    """
    return base_url.rstrip("/")


async def request_json(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    """
    Sends one request to the warehouse API.

    Args:
        client: warehouse API client.
        method: HTTP method.
        url: path relative to the warehouse base URL.
        kwargs: extra httpx arguments, e.g. json.

    Returns:
        The decoded JSON response.
    """
    response = await client.request(method, url, **kwargs)
    response.raise_for_status()
    return response.json()


async def list_repositories(client: httpx.AsyncClient) -> list[dict]:
    """
    Lists all repositories (the owners that endpoints belong to).

    Args:
        client: warehouse API client.

    Returns:
        List of repository dicts, ordered by code.
    """
    repositories = await request_json(client, "GET", "/repositories")
    return sorted(repositories, key = lambda repository: repository["code"])


async def find_repository_id(client: httpx.AsyncClient, repository_code: str) -> int | None:
    """
    Looks up the id of a repository by its code (the value endpoints.repository_id must point to).

    Args:
        client: warehouse API client.
        repository_code: repositories.code.

    Returns:
        The repository id, or None if the code is unknown.
    """
    return next((r["id"] for r in await list_repositories(client) if r["code"] == repository_code), None)


async def insert_repository(
    client: httpx.AsyncClient, code: str, name: str, description: str | None, base_url: str | None,
) -> dict:
    """
    Creates a new repository so endpoints can be attached to it.

    Args:
        client: warehouse API client.
        code: unique short code.
        name: human-readable repository name.
        description: free-text description of the repository.
        base_url: URL of the repository's own website (not the OAI-PMH endpoint).

    Returns:
        The created repository as a dict.
    """
    return await request_json(client, "POST", "/repositories", json = {
        "code": code,
        "name": name,
        "description": description,
        "base_url": base_url,
    })


async def list_endpoints(client: httpx.AsyncClient, state: str = "all") -> list[dict]:
    """
    Lists endpoints filtered by state; each dict also gets "metadata_prefix" taken from harvest_params.

    Args:
        client: warehouse API client.
        state: one of "all", "waiting", "approved", "failed".

    Returns:
        List of endpoint dicts, newest first.
    """
    endpoints = [
        {**endpoint, "metadata_prefix": (endpoint.get("harvest_params") or {}).get("metadata_prefix")}
        for endpoint in await request_json(client, "GET", "/endpoints")
    ]
    matching = [endpoint for endpoint in endpoints if STATE_FILTERS[state](endpoint["is_active"])]
    return sorted(matching, key = lambda endpoint: endpoint["id"], reverse = True)


async def find_by_harvest_url(client: httpx.AsyncClient, harvest_url: str) -> dict | None:
    """
    Looks for an endpoint with the same URL, ignoring trailing slashes stored in the warehouse.

    Args:
        client: warehouse API client.
        harvest_url: already normalized URL.

    Returns:
        The matching endpoint dict, or None.
    """
    endpoints = await list_endpoints(client)
    return next((e for e in endpoints if normalize_url(e["harvest_url"]) == harvest_url), None)


async def get_endpoint(client: httpx.AsyncClient, endpoint_id: int) -> dict | None:
    """
    Loads one endpoint.

    Args:
        client: warehouse API client.
        endpoint_id: id of the endpoint.

    Returns:
        The endpoint dict (including "metadata_prefix"), or None if it does not exist.
    """
    return next((e for e in await list_endpoints(client) if e["id"] == endpoint_id), None)


async def insert_endpoint(
    client: httpx.AsyncClient, repository_id: int, name: str, harvest_url: str, scientific_discipline: str,
    is_active: bool | None, metadata_prefix: str | None, sets: list[str] | None, harvest_interval_days: int,
    description: str,
) -> dict:
    """
    Creates a new OAI-PMH endpoint in the warehouse.

    Args:
        client: warehouse API client.
        repository_id: id of the owning repository (see find_repository_id).
        name: unique endpoint name.
        harvest_url: normalized OAI-PMH base URL.
        scientific_discipline: discipline label.
        is_active: None = waiting for review, True = approved, False = failed.
        metadata_prefix: metadata prefix selected by the submitter or auto-detected, or None.
        sets: OAI-PMH sets selected by the submitter, or None to harvest the whole repository.
        harvest_interval_days: harvest schedule in days.
        description: validation result or failure reason.

    Returns:
        The created endpoint as a dict.
    """
    # harvest_params mirrors the warehouse's HarvestParams model: only metadata_prefix is
    # required there. "set" is stored only when the submitter selected sets, and
    # additional_metadata_params has no submitter-facing option, so it is never set here.
    harvest_params: dict = {"metadata_prefix": metadata_prefix or ""}
    if sets:
        harvest_params["set"] = sets
    return await request_json(client, "POST", "/endpoints", json = {
        "repository_id": repository_id,
        "name": name,
        "harvest_url": harvest_url,
        "protocol": "OAI-PMH",
        "scientific_discipline": scientific_discipline,
        "is_active": is_active,
        "harvest_params": harvest_params,
        "harvest_interval_days": harvest_interval_days,
        "description": description,
    })


async def update_validation_result(client: httpx.AsyncClient, endpoint_id: int, is_active: bool | None, metadata_prefix: str | None, description: str) -> dict:
    """
    Stores a validation result on an existing endpoint (the prefix in harvest_params is kept if None).

    Args:
        client: warehouse API client.
        endpoint_id: id of the endpoint.
        is_active: None = waiting for review, True = approved, False = failed.
        metadata_prefix: metadata prefix found by validation, or None.
        description: validation result, failure reason or approval note.

    Returns:
        The updated endpoint as a dict.
    """
    changes = {"is_active": is_active, "description": description}
    if metadata_prefix:
        current = await get_endpoint(client, endpoint_id)
        changes["harvest_params"] = {**(current["harvest_params"] or {}), "metadata_prefix": metadata_prefix}
    return await request_json(client, "PATCH", f"/endpoints/{endpoint_id}", json = changes)


async def count_endpoints(client: httpx.AsyncClient) -> dict:
    """
    Counts endpoints per state.

    Args:
        client: warehouse API client.

    Returns:
        dict with "waiting", "approved", "failed" and "total" counts.
    """
    endpoints = await list_endpoints(client)
    counts = {state: sum(STATE_FILTERS[state](e["is_active"]) for e in endpoints) for state in ("waiting", "approved", "failed")}
    return {**counts, "total": len(endpoints)}