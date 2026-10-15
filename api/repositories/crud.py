import json
from datetime import timedelta

import asyncpg

# is_active column: NULL = waiting for admin review, TRUE = approved, FALSE = failed.
STATE_FILTERS = {
    "all": "TRUE",
    "waiting": "is_active IS NULL",
    "approved": "is_active IS TRUE",
    "failed": "is_active IS FALSE",
}

# Endpoint rows come back with the configured metadata prefix pulled out of harvest_params.
ENDPOINT_COLUMNS = "*, harvest_params->>'metadata_prefix' AS metadata_prefix"


def normalize_url(base_url: str) -> str:
    """
    Removes trailing slashes so equal URLs compare equal.

    Args:
        base_url: URL as entered by the user.

    Returns:
        The URL without trailing slashes.
    """
    return base_url.rstrip("/")



async def find_repository_id(pool: asyncpg.Pool, repository_code: str) -> int | None:
    """
    Looks up the id of a repository by its code.

    Args:
        pool: database pool.
        repository_code: repositories.code.

    Returns:
        The repository id, or None if the code is unknown.
    """
    return await pool.fetchval("SELECT id FROM repositories WHERE code = $1", repository_code)



async def list_repositories(pool: asyncpg.Pool) -> list[dict]:
    """
    Lists all repositories.

    Args:
        pool: database pool.

    Returns:
        List of repository rows as dicts, ordered by code.
    """
    rows = await pool.fetch("SELECT * FROM repositories ORDER BY code")
    return [dict(row) for row in rows]



async def insert_repository(pool: asyncpg.Pool, code: str, name: str) -> dict:
    """
    Inserts a new repository so endpoints can be attached to it.

    Args:
        pool: database pool.
        code: unique short code.
        name: human-readable repository name.

    Returns:
        The inserted row as a dict.
    """
    row = await pool.fetchrow("INSERT INTO repositories (code, name) VALUES ($1, $2) RETURNING *", code, name)
    return dict(row)



async def find_by_harvest_url(pool: asyncpg.Pool, harvest_url: str) -> dict | None:
    """
    Looks for an endpoint with the same URL.

    Args:
        pool: database pool.
        harvest_url: normalized URL.

    Returns:
        The matching endpoint row as a dict, or None.
    """
    row = await pool.fetchrow(f"SELECT {ENDPOINT_COLUMNS} FROM endpoints WHERE rtrim(harvest_url, '/') = $1", harvest_url)
    return dict(row) if row else None



async def get_endpoint(pool: asyncpg.Pool, endpoint_id: int) -> dict | None:
    """
    Loads one endpoint.

    Args:
        pool: database pool.
        endpoint_id: id of the endpoint.

    Returns:
        The endpoint row as a dict (including "metadata_prefix"), or None if it does not exist.
    """
    row = await pool.fetchrow(f"SELECT {ENDPOINT_COLUMNS} FROM endpoints WHERE id = $1", endpoint_id)
    return dict(row) if row else None



async def insert_endpoint(pool: asyncpg.Pool, repository_id: int, name: str, harvest_url: str, 
                          scientific_discipline: str, is_active: bool | None, metadata_prefix: str | None, 
                          harvest_interval_days: int, description: str) -> dict:
    """
    Inserts a new OAI-PMH endpoint into the endpoints table.

    Args:
        pool: database pool.
        repository_id: id of the repository.
        name: unique endpoint name.
        harvest_url: normalized OAI-PMH base URL.
        scientific_discipline: discipline label.
        is_active: None = waiting for review, True = approved, False = failed.
        metadata_prefix: detected metadata prefix, or None.
        harvest_interval_days: harvest schedule in days.
        description: validation result or failure reason.

    Returns:
        The inserted row as a dict.
    """
    harvest_params = {"metadata_prefix": metadata_prefix} if metadata_prefix else {}
    row = await pool.fetchrow(
        """
        INSERT INTO endpoints
            (repository_id, name, harvest_url, protocol, scientific_discipline,
             is_active, harvest_params, harvest_schedule, description)
        VALUES ($1, $2, $3, 'OAI-PMH', $4, $5, $6, $7, $8)
        RETURNING *
        """,
        repository_id, name, harvest_url, scientific_discipline, is_active,
        json.dumps(harvest_params), timedelta(days = harvest_interval_days), description,
    )
    return dict(row)



async def update_validation_result(pool: asyncpg.Pool, endpoint_id: int, is_active: bool | None, 
                                   metadata_prefix: str | None, description: str) -> dict:
    """
    Stores a validation result on an existing endpoint.

    Args:
        pool: database pool.
        endpoint_id: id of the endpoint.
        is_active: None = waiting for review, True = approved, False = failed.
        metadata_prefix: metadata prefix found by validation, or None.
        description: validation result, failure reason or approval note.

    Returns:
        The updated row as a dict.
    """
    row = await pool.fetchrow(
        """
        UPDATE endpoints
        SET is_active = $2,
            description = $3,
            harvest_params = CASE 
                WHEN $4::text IS NULL THEN harvest_params
                ELSE harvest_params || jsonb_build_object('metadata_prefix', $4::text) 
            END
        WHERE id = $1
        RETURNING *
        """,
        endpoint_id, is_active, description, metadata_prefix,
    )
    return dict(row)



async def list_endpoints(pool: asyncpg.Pool, state: str) -> list[dict]:
    """
    Lists endpoints filtered by state.

    Args:
        pool: database pool.
        state: one of "all", "waiting", "approved", "failed".

    Returns:
        List of endpoint rows as dicts, newest first.
    """
    rows = await pool.fetch(f"SELECT {ENDPOINT_COLUMNS} FROM endpoints WHERE {STATE_FILTERS[state]} ORDER BY id DESC")
    return [dict(row) for row in rows]



async def count_endpoints(pool: asyncpg.Pool) -> dict:
    """
    Counts endpoints per state.

    Args:
        pool: database pool.

    Returns:
        dict with "waiting", "approved", "failed" and "total" counts.
    """
    row = await pool.fetchrow(
        """
        SELECT count(*) FILTER (WHERE is_active IS NULL)  AS waiting,
               count(*) FILTER (WHERE is_active IS TRUE)  AS approved,
               count(*) FILTER (WHERE is_active IS FALSE) AS failed,
               count(*)                                   AS total
        FROM endpoints
        """
    )
    return dict(row)