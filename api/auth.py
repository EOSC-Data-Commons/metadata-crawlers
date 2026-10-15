from fastapi import Depends, Header, HTTPException


async def get_current_user(authorization: str | None = Header(default = None)) -> dict:
    """
    Identifies who is calling the API.

    Args:
        authorization: value of the Authorization header sent by the client.

    Returns:
        dict with at least "username" and "role" (e.g. "admin", "submitter").
    """
    # if authorization is None:
    #     raise HTTPException(status_code = 401, detail = "Login required")
    # TODO: replace this placeholder with your real login (decode the token / look up the session).
    # As written it accepts ANY header value, so do not deploy it.
    return {"username": "dev-user", "role": "admin"}



def require_role(*allowed_roles: str):
    """
    Builds a dependency that only lets users with one of the given roles through.

    Args:
        allowed_roles: role names that may call the route.

    Returns:
        A FastAPI dependency that returns the current user dict.
    """
    async def check_role(user: dict = Depends(get_current_user)) -> dict:
        if user["role"] not in allowed_roles:
            raise HTTPException(status_code = 403, detail = "You are not allowed to do this")
        return user

    return check_role