from .base import SourceAdapter
from .datacite import add_orcid, to_datacite_oai_xml
from .http import get_json, make_client
from .json_utils import as_list, strip_doi, text, unwrap
from .models import (
    CommonRecord,
    Contributor,
    DateEntry,
    Description,
    FundingReference,
    HarvestedRecord,
    Person,
    RelatedIdentifier,
    Rights,
)
from .runner import run_harvester

__all__ = [
    "SourceAdapter",
    "add_orcid",
    "to_datacite_oai_xml",
    "get_json",
    "make_client",
    "as_list",
    "strip_doi",
    "text",
    "unwrap",
    "CommonRecord",
    "Contributor",
    "DateEntry",
    "Description",
    "FundingReference",
    "HarvestedRecord",
    "Person",
    "RelatedIdentifier",
    "Rights",
    "run_harvester",
]