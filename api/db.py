"""
Database connection pool lifecycle.

create_pool()/close_pool() are called from main.py's lifespan hook on
app startup/shutdown. get_pool() is a FastAPI dependency that route
handlers use to reach the pool without importing `app` directly.
"""

import asyncpg
from fastapi import Request

from config import DATABASE_URL



async def create_pool() -> asyncpg.Pool:
    """Open a single asyncpg connection pool, reused by every request."""
    return await asyncpg.create_pool(DATABASE_URL, min_size = 1, max_size = 5)



async def close_pool(pool: asyncpg.Pool) -> None:
    """Close the pool cleanly on app shutdown."""
    await pool.close()



async def get_pool(request: Request) -> asyncpg.Pool:
    """FastAPI dependency: fetch the pool stashed on app.state by main.py."""
    return request.app.state.pool