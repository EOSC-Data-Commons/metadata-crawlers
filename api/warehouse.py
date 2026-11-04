"""HTTP client for the warehouse API, shared by all requests (replaces the old database pool)."""

import httpx
from fastapi import Request

from settings import settings


def create_client() -> httpx.AsyncClient:
    """
    Creates the shared warehouse API client.

    Returns:
        An httpx.AsyncClient whose base URL and timeout come from settings.
    """
    return httpx.AsyncClient(base_url = settings.WAREHOUSE_API_URL, timeout = settings.WAREHOUSE_API_TIMEOUT)


async def get_client(request: Request) -> httpx.AsyncClient:
    """
    FastAPI dependency that hands routes the client stored on app.state.

    Args:
        request: the current request.

    Returns:
        The shared warehouse API client.
    """
    return request.app.state.warehouse