import json
from pathlib import Path

import jq

# This file lives in <module root>/aggregator_initial_harvest/json_manipulation/
_MODULE_ROOT = Path(__file__).resolve().parents[1]
_RESOURCES_FOLDER = _MODULE_ROOT / "resources"

zenodo_datacite_jq_path = _RESOURCES_FOLDER / "zenodo_to_datacite.jq"
zenodo_datacite_transformer = jq.compile(zenodo_datacite_jq_path.read_text())

with open(_RESOURCES_FOLDER / "datacite_schema.json") as f:
    datacite_schema = json.load(f)

zenodo_files_jq_path = _RESOURCES_FOLDER / "zenodo_dump_to_files_endpoint.jq"
zenodo_files_transformer = jq.compile(zenodo_files_jq_path.read_text())

with open(_RESOURCES_FOLDER / "zenodo_files_schema.json") as f:
    files_schema = json.load(f)

def transform_json_zenodo_to_datacite(zenodo_json: dict) -> str:
    """
    Transform Zenodo raw JSON to DataCite schema JSON.
    The result is stringified JSON.

    Args:
        zenodo_json: dict, a JSON object, raw input from the Zenodo dump
    """
    result = zenodo_datacite_transformer.input_value(zenodo_json)

    return result.text()

def transform_json_zenodo_to_files_endpoint(zenodo_json: dict) -> str:
    """
    Transform Zenodo raw JSON to the JSON returned by the /files endpoint.  
    This is needed for "additional_metadata" in the warehouse, so it needs to be in the same schema.
    The result is stringified JSON.

    Args:
        zenodo_json: dict, a JSON object, raw input from the Zenodo dump
    """
    result = zenodo_files_transformer.input_value(zenodo_json)

    return result.text()