from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Person:
    """A creator or the person part of a contributor."""
    name: str
    orcid: str | None = None
    affiliation: str | None = None
    name_type: str = "Personal"


@dataclass
class Contributor:
    person: Person
    contributor_type: str  # DataCite contributorType, e.g. "ProjectLeader"


@dataclass
class DateEntry:
    date_type: str  # DataCite dateType, e.g. "Submitted", "Available", "Updated"
    value: str      # already formatted (YYYY-MM-DD)


@dataclass
class RelatedIdentifier:
    id_type: str    # e.g. "DOI", "URL"
    relation: str   # e.g. "IsDocumentedBy"
    value: str


@dataclass
class Rights:
    text: str
    uri: str | None = None
    identifier: str | None = None


@dataclass
class Description:
    text: str
    description_type: str  # e.g. "Abstract", "TechnicalInfo"


@dataclass
class FundingReference:
    funder_name: str
    award_number: str | None = None


@dataclass
class CommonRecord:
    """DataCite-shaped metadata. Empty lists / None mean "omit this element"."""
    # mandatory DataCite properties
    identifier: str
    identifier_type: str            # "DOI", "URL", ...
    title: str | None
    publisher: str
    resource_type: str              # free-text part of <resourceType>
    resource_type_general: str = "Dataset"
    publication_year: str | None = None
    creators: list[Person] = field(default_factory = list)

    # recommended / optional
    language: str | None = None
    contributors: list[Contributor] = field(default_factory = list)
    dates: list[DateEntry] = field(default_factory = list)
    related_identifiers: list[RelatedIdentifier] = field(default_factory = list)
    sizes: list[str] = field(default_factory = list)
    subjects: list[str] = field(default_factory = list)
    formats: list[str] = field(default_factory = list)
    rights: list[Rights] = field(default_factory = list)
    descriptions: list[Description] = field(default_factory = list)
    funding_references: list[FundingReference] = field(default_factory = list)


@dataclass
class HarvestedRecord:
    """Everything the runner needs to send one harvest event."""
    record_id: str
    datestamp: str                  # YYYY-MM-DD, used for incremental harvesting
    metadata: CommonRecord
    additional_metadata: Any = None
    is_deleted: bool = False
