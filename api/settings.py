"""Where the warehouse API lives. The defaults work without a .env file."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file = ".env", extra = "ignore")

    WAREHOUSE_API_URL: str = "http://localhost:8080"
    WAREHOUSE_API_TIMEOUT: int = 30

    # EGI Check-in (OIDC).
    CHECKIN_ISSUER: str = "https://aai.egi.eu/auth/realms/egi"
    # Public client registered at EGI Check-in. Redirect URI to register: <api url>/docs/oauth2-redirect
    CHECKIN_CLIENT_ID: str = ""


settings = Settings()