"""
OAI-PMH onboarding API.

Run it with:
    uvicorn main:app --reload --port 8000

Then open http://127.0.0.1:8000/docs to try it interactively

Requires:
    - A .env file defining USER, PASSWORD, and DATABASE - the same
      credentials used to connect to this Postgres server from pgAdmin
      or psql (see config.py).
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from db import create_pool, close_pool
from repositories.routes import router as repositories_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs once on startup and once on shutdown. Opens a single asyncpg
    connection pool and stashes it on app.state so every request handler
    can reuse it (via the get_pool dependency) instead of opening a new
    database connection per request.
    """
    app.state.pool = await create_pool()
    yield
    await close_pool(app.state.pool)


app = FastAPI(title = "OAI-PMH Onboarding API", lifespan = lifespan)
app.include_router(repositories_router)