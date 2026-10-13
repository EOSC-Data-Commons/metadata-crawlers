from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterator

from .models import HarvestedRecord


class SourceAdapter(ABC):
    """
    What a repository-specific harvester has to provide.

    The work is split in two so the runner can isolate failures per record:

    * `iter_raw` does the listing / pagination / incremental filtering and
      yields one lightweight item per record (whatever the repository gives
      you: a dict, an accession, an (id, dict) tuple, ...).
    * `build_record` turns one raw item into a `HarvestedRecord`. This is where
      the extra per-record requests (file lists, sizes, keywords, ...) happen.
      If it raises, only that record is counted as failed.
    """

    #: Short name used in log messages
    name: str = "unknown"

    @abstractmethod
    def iter_raw(self, from_date: str | None, until_date: str) -> Iterator[Any]:
        """Yield raw items. `from_date` is None for a full harvest."""

    @abstractmethod
    def build_record(self, raw: Any) -> HarvestedRecord:
        """Build the full record (metadata + files) for one raw item."""
