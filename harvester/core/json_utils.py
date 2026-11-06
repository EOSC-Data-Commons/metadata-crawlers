"""
Helpers for reading the nested JSON that EBI APIs return.

EMDB (and other EBI services generated from XML) write an element's text under
`"valueOf_"` when the element also has attributes, so the same kind of value can
appear in several shapes:

    "title": "In situ structure ..."                      plain string
    "code": {"valueOf_": "REL"}                           wrapped text
    "author": {"ORCID": "0000-...", "valueOf_": "Appel L"}  wrapped text + attribute
    "a": {"units": "Å", "valueOf_": "221.56"}             wrapped text + unit

Use `text()` to read any of these as a clean string. The examples in the
docstrings below are doctests: `python -m doctest harvesters/core/json_utils.py`.
"""

from __future__ import annotations

from typing import Any


def unwrap(value: Any) -> Any:
    """
    Return the plain value inside a possibly wrapped JSON value, in its original type.

    >>> unwrap({"units": "Å", "valueOf_": "221.56"})
    '221.56'
    >>> unwrap([{"valueOf_": "first"}, {"valueOf_": "second"}])   # a list gives its first item
    'first'
    >>> unwrap({"instance_type": "grant_reference"})              # a dict with no text key
    >>> unwrap(6244)                                              # anything else is returned as is
    6244
    """
    if isinstance(value, dict):
        for key in ("valueOf_", "#text", "value"):
            if key in value:
                return unwrap(value[key])
        return None
    if isinstance(value, list):
        return unwrap(value[0]) if value else None
    return value


def text(value: Any) -> str | None:
    """
    Read a value from the JSON as a stripped, non-empty string, or None if there is none.
    Use this for any text field; None means "not given", so callers can simply
    write `if text(x):`.

    >>> text("Appel L")
    'Appel L'
    >>> text({"ORCID": "0000-0002-7311-2973", "valueOf_": "Appel L"})
    'Appel L'
    >>> text({"valueOf_": "REL"})
    'REL'
    >>> text(6244)
    '6244'
    >>> text("   ") is None, text(None) is None, text([]) is None
    (True, True, True)
    """
    unwrapped = unwrap(value)
    if unwrapped is None:
        return None
    return str(unwrapped).strip() or None


def as_list(value: Any) -> list[Any]:
    """
    Always return a list. The JSON may give a single object where it could give
    several, so this lets callers loop without checking which one they got.

    >>> as_list(None)
    []
    >>> as_list({"emdb_id": "EMD-1"})
    [{'emdb_id': 'EMD-1'}]
    >>> as_list([1, 2])
    [1, 2]
    """
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def strip_doi(doi: str | None) -> str | None:
    """
    Remove a leading `doi:` prefix, so the value is a bare DOI.

    >>> strip_doi("doi:10.1038/s41586-026-11016-2")
    '10.1038/s41586-026-11016-2'
    >>> strip_doi("10.1038/s41586-026-11016-2")
    '10.1038/s41586-026-11016-2'
    >>> strip_doi(None) is None
    True
    """
    if not doi:
        return None
    return doi.split("doi:")[-1] if doi.lower().startswith("doi:") else doi