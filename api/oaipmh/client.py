import xml.etree.ElementTree as ET
import httpx

# The OAI-PMH XML namespace. Every element returned by a compliant OAI-PMH
# repository lives in this namespace, so every ElementTree lookup has to
# be prefixed with it (e.g. f"{OAI_NS}repositoryName") or the lookup will
# silently find nothing.
OAI_NS = "{http://www.openarchives.org/OAI/2.0/}"


class OaiPmhError(Exception):
    """
    Raised when a repository responds with HTTP 200 but the XML body
    itself is an OAI-PMH <error> element (e.g. badVerb, badArgument,
    noRecordsMatch), rather than the data we asked for.
    """

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def check_for_oai_error(root: ET.Element) -> None:
    """
    OAI-PMH repositories report protocol-level errors (bad verb, bad
    argument, no records matching, etc.) as an <error code="..."> element
    inside an otherwise-normal 200 OK response, NOT as an HTTP error
    status.

    Raises OaiPmhError if an <error> element is found; otherwise returns None.
    """

    # .// searches all descendants of root, regardless of depth.
    # root.find() returns the first matching element, or None if no match exists.
    error_el = root.find(f".//{OAI_NS}error")
    if error_el is not None:
        code = error_el.get("code", "unknown")
        message = (error_el.text or "").strip() or "no message provided"
        raise OaiPmhError(code, message)


async def fetch_oai(client: httpx.AsyncClient, harvest_url: str, **params) -> ET.Element:
    """
    Make a single OAI-PMH request and return the parsed XML root element.

    `**params` becomes the query string - e.g. calling
    fetch_oai(client, url, verb = "Identify") sends "?verb=Identify".

    This function is the single choke point where every OAI-PMH response
    passes through both:
      1. resp.raise_for_status() - catches transport-level failures
         (connection refused, 404, 500, timeouts, etc.)
      2. check_for_oai_error(root) - catches protocol-level failures
         that still return HTTP 200 (see check_for_oai_error)
    """
    resp = await client.get(harvest_url, params = params)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    check_for_oai_error(root)
    return root