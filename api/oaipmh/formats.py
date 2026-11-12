"""
Metadata prefixes the onboarding API and the harvest pipeline cover, and the
XML namespaces accepted for each of them.

This is the single source of truth used by:
- routes.py, to reject unsupported prefix choices, describe what we cover and
  mark the formats a repository offers as supported or not,
- probe.py, to validate sample records against the format family of the
  SELECTED prefix rather than the exact namespace the repository advertises.

Namespaces are matched by prefix (startswith), so schema VERSIONS do not
matter: any DataCite kernel namespace (kernel-2.2, kernel-3, kernel-4, ...)
counts as DataCite, while a Dublin Core record does not, and vice versa. This
keeps validation tolerant of version differences while still catching
repositories that advertise one format but serve another.
"""

# Format family -> namespace prefixes accepted for that family. Keep the
# patterns broad enough to cover every schema version of the family.
NAMESPACE_FAMILIES = {
    # DataCite: any kernel version, plus the OAI-PMH DataCite profile container
    # (<oai_datacite> wraps a <payload> holding the actual <resource>).
    "DataCite": ("http://datacite.org/schema/kernel-", "http://schema.datacite.org/oai/oai-"),
    # Dublin Core: the qualified oai_dc container and the bare dc elements.
    "Dublin Core": ("http://www.openarchives.org/OAI/2.0/oai_dc/", "http://purl.org/dc/elements/1.1/"),
    # DDI: any 2.x codebook version (2_0, 2_1, 2_5, ...).
    "DDI": ("ddi:codebook:",),
}

# Metadata prefixes the pipeline can harvest, in order of preference, mapped
# to the format family their records must be in. Keep in sync with
# harvester/harvester_oaipmh.py: oai_datacite / oai_datacite4 / datacite are
# ingested as DataCite directly, oai_dc is transformed with dc_to_datacite.xsl
# and oai_ddi25 with ddi_to_datacite.xsl.
SUPPORTED_PREFIXES = {
    "oai_datacite": "DataCite",
    "oai_datacite4": "DataCite",
    "datacite": "DataCite",
    "oai_dc": "Dublin Core",
    "oai_ddi25": "DDI",
}

# Order in which a prefix is picked when the submitter does not select one.
PREFERRED_PREFIXES = list(SUPPORTED_PREFIXES)


def is_supported_prefix(metadata_prefix: str | None) -> bool:
    """
    Tells whether the harvest pipeline can handle a metadata prefix.

    Args:
        metadata_prefix: prefix as used in OAI-PMH metadataPrefix arguments.

    Returns:
        True if the prefix is one of SUPPORTED_PREFIXES.
    """
    return metadata_prefix in SUPPORTED_PREFIXES


def is_accepted_namespace(namespace: str, metadata_prefix: str) -> bool:
    """
    Tells whether an XML namespace belongs to the format family of a prefix.

    Matching is done by prefix, so any schema version of the family is
    accepted (e.g. every http://datacite.org/schema/kernel-* namespace).

    Args:
        namespace: namespace URI found in a record.
        metadata_prefix: the selected metadata prefix.

    Returns:
        True if the namespace matches the prefix's format family; False for
        unsupported prefixes or unrelated namespaces.
    """
    family = SUPPORTED_PREFIXES.get(metadata_prefix or "")
    return family is not None and any(namespace.startswith(pattern) for pattern in NAMESPACE_FAMILIES[family])


def describe_family(namespace: str) -> str | None:
    """
    Names the format family an XML namespace belongs to.

    Args:
        namespace: namespace URI found in a record.

    Returns:
        Family name such as "DataCite", or None when the namespace belongs to
        no known family.
    """
    return next(
        (family for family, patterns in NAMESPACE_FAMILIES.items() if namespace.startswith(patterns)),
        None,
    )