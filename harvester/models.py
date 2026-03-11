from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

# NOTE: models copied from metadata-warehouse `src/transform.py`
# https://github.com/EOSC-Data-Commons/metadata-warehouse/blob/main/src/transform.py
# To be used to parse responses from the warehouse API

class AdditionalMetadataParams(BaseModel):
    format: str
    endpoint: str
    protocol: str


class HarvestParams(BaseModel):
    metadata_prefix: str
    set: Optional[list[str]]
    additional_metadata_params: Optional[AdditionalMetadataParams]


class EndpointConfig(BaseModel):
    name: str
    harvest_url: str
    harvest_params: HarvestParams
    code: str
    protocol: str


class HarvestRunCreateResponse(BaseModel):
    """Retrieve harvest run info from the warehouse API response after starting a new harvest run."""

    id: str = Field(description='ID of the new harvest run')
    from_date: Optional[datetime] = Field(None, description='From date for selective harvesting')
    until_date: datetime = Field(description='Until date for selective harvesting')
    endpoint_config: EndpointConfig = Field(description='Description of the endpoint used for harvesting')


# TODO: Used for send_harvest_event
class HarvestEventCreateRequest(BaseModel):
    """Used for send_harvest_event"""

    record_identifier: str
    datestamp: datetime
    raw_metadata: str  # XML
    additional_metadata: Optional[str] = None  # XML or JSON (stringified)
    harvest_url: str
    repo_code: str
    harvest_run_id: str
    is_deleted: bool
