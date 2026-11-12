"""Login with EGI Check-in and role checks for the API."""

import re

import httpx
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2AuthorizationCodeBearer

from settings import settings

VO = "vo.eosc-data-commons.eu"
ENROLL_URL = f"https://aai.egi.eu/auth/realms/id/account/#/enroll?groupPath=/{VO}"

# API role -> role name inside the VO. Highest role first; TODO: check.
VO_ROLES = {"admin": "admin", "submitter": "onboard"}

# Entitlement format: <NAMESPACE>:group:<VO>[:<SUBGROUP>*][:role=<ROLE>]#<GROUP-AUTHORITY>
ENTITLEMENT_PATTERN = re.compile(
    r"^urn:mace:egi\.eu:group:(?P<vo>[^:#]+)(?P<subgroups>(?::[^:#=]+)*)(?::role=(?P<role>[^:#]+))?#aai\.egi\.eu$"
)

# Makes /docs show an "Authorize" button that logs in through EGI Check-in (authorization code flow).
bearer_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl = f"{settings.CHECKIN_ISSUER}/protocol/openid-connect/auth",
    tokenUrl = f"{settings.CHECKIN_ISSUER}/protocol/openid-connect/token",
    scopes = {"openid": "Sign in", "eduperson_entitlement": "Read your VO roles"},
)


def roles_in_vo(entitlements: list[str]) -> set[str]:
    """
    Reads the roles a user holds directly in our VO (roles inside subgroups are ignored).

    Args:
        entitlements: values of the eduperson_entitlement claim.

    Returns:
        Set of role names, e.g. {"onboard"}.
    """
    matches = (ENTITLEMENT_PATTERN.match(entitlement) for entitlement in entitlements)
    return {m["role"] for m in matches if m and m["vo"] == VO and not m["subgroups"] and m["role"]}


async def get_current_user(access_token: str = Depends(bearer_scheme)) -> dict:
    """
    Identifies the caller by asking EGI Check-in about the access token and reading the caller's VO role.

    Args:
        access_token: the access token from the "Authorization: Bearer ..." header (filled in by the /docs login).

    Returns:
        dict with "username" and "role" ("admin" or "submitter").
    """
    userinfo_url = f"{settings.CHECKIN_ISSUER}/protocol/openid-connect/userinfo"
    async with httpx.AsyncClient(timeout = 10) as client:
        response = await client.get(userinfo_url, headers = {"Authorization": f"Bearer {access_token}"})
    if response.status_code != 200:
        raise HTTPException(status_code = 401, detail = "Invalid or expired token")

    userinfo = response.json()
    vo_roles = roles_in_vo(userinfo.get("eduperson_entitlement", []))
    api_role = next((api_role for api_role, vo_role in VO_ROLES.items() if vo_role in vo_roles), None)
    if api_role is None:
        raise HTTPException(
            status_code = 403,
            detail = f"You need the '{VO_ROLES['submitter']}' role in {VO}. Request it at {ENROLL_URL}",
        )
    return {"username": userinfo.get("preferred_username") or userinfo["sub"], "role": api_role}


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