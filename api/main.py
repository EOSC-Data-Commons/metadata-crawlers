"""
OAI-PMH onboarding API - app wiring only.

Run it with:
    uvicorn main:app --reload --port 8000

Then open http://127.0.0.1:8000/docs to try it interactively.

It talks to the warehouse API (see settings.py, default http://localhost:8080)
and needs no database credentials.
"""

from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from repositories.routes import router as repositories_router
from settings import settings
from warehouse import create_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Opens one warehouse API client on startup and closes it on shutdown.

    Args:
        app: the FastAPI application.
    """
    app.state.warehouse = create_client()
    yield
    await app.state.warehouse.aclose()


app = FastAPI(
    title = "OAI-PMH Onboarding API",
    lifespan = lifespan,
    # Defines the groups shown in /docs and the text rendered under each group header.
    # The order here is the order the groups appear in; routes carry matching `tags`.
    openapi_tags = [
        {
            "name": "Repositories",
            "description": (
                "Top-level containers that OAI-PMH endpoints are attached to. "
                "Create these first: a repository's unique short code is required when submitting an endpoint. "
                "Creating repositories is admin-only; admins and submitters can list them."
            ),
        },
        {
            "name": "Metadata prefixes",
            "description": (
                "Find out which metadata formats a repository offers and which of them we can "
                "harvest, before submitting an endpoint. We cover `oai_datacite`, `oai_datacite4` "
                "and `datacite` (DataCite), `oai_dc` (Dublin Core) and `oai_ddi25` (DDI). "
                "Records are validated against the format family of the selected prefix, so "
                "schema versions do not matter (any DataCite kernel or DDI 2.x version is fine) - "
                "but records served in a different format than the selected one (e.g. Dublin Core "
                "records under a `datacite` prefix) fail validation."
            ),
        },
        {
            "name": "Endpoint submission",
            "description": (
                "Add a new OAI-PMH endpoint to an existing repository. Call "
                "GET /endpoints/metadata-formats first to see which metadata prefixes the "
                "repository offers, then pass the one you want harvested as `metadata_prefix` "
                "(we cover `oai_datacite`, `oai_datacite4`, `datacite`, `oai_dc` and `oai_ddi25`; "
                "if you omit it, the best supported one is picked automatically). The endpoint is "
                "probed end to end (all six OAI-PMH verbs, sample records in the selected format, "
                "incremental harvesting, resumption-token paging) before it is stored: it lands as "
                "waiting (`is_active` NULL) if the probe passed, or failed (`FALSE`) if it did not."
            ),
        },
        {
            "name": "Endpoint validation",
            "description": (
                "Re-run the OAI-PMH probe on an endpoint that is already on board, "
                "found by its base URL and using its configured metadata prefix. "
                "The validation result and the `is_active` state are updated accordingly."
            ),
        },
        {
            "name": "Endpoint review",
            "description": (
                "Inspect the onboarding queue: list endpoints (optionally only the waiting, "
                "approved or failed ones), see counts per state, "
                "and approve waiting endpoints so the harvest pipeline starts harvesting them."
            ),
        },
    ],
    swagger_ui_init_oauth = {
        "clientId": settings.CHECKIN_CLIENT_ID,
        "scopes": "openid eduperson_entitlement",
        "usePkceWithAuthorizationCodeGrant": True,
    },
)
app.include_router(repositories_router)


@app.exception_handler(httpx.RequestError)
async def warehouse_unreachable(request: Request, error: httpx.RequestError) -> JSONResponse:
    """
    Turns a failed connection to the warehouse API into a readable 502 response.

    Args:
        request: the current request.
        error: the httpx error that was raised.

    Returns:
        A 502 JSON response.
    """
    return JSONResponse(status_code = 502, content = {"detail": f"Warehouse API not reachable: {error!r}"})


@app.exception_handler(httpx.HTTPStatusError)
async def warehouse_rejected(request: Request, error: httpx.HTTPStatusError) -> JSONResponse:
    """
    Turns an error status from the warehouse API (e.g. 404 for a missing route) into a readable 502 response.

    Args:
        request: the current request.
        error: the httpx error that was raised.

    Returns:
        A 502 JSON response naming the warehouse call that failed.
    """
    warehouse_request = error.request
    return JSONResponse(status_code = 502, content = {
        "detail": f"Warehouse API answered {error.response.status_code} for {warehouse_request.method} {warehouse_request.url}",
    })