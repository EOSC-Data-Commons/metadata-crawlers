# script for harvesting metadata based on oaipmh-scythe client
# run in terminal with the path to config file as argument: python harvester_scheduled.py {repos_config/repo.json}

import os
import sys
import argparse
from datetime import datetime
from lxml import etree as ET
import json
from oaipmh_scythe import Scythe
import requests
import traceback
from typing import Dict, Optional

NS = {"oai": "http://www.openarchives.org/OAI/2.0/"}
API_BASE_URL = ""

def load_repo_config(harvest_url: str, timeout: int = 30) -> Dict:
    """
    Fetch repository configuration from API.

    :param harvest_url: endpoint for harvesting
    :return: json file with config data for the matching repository
    """
    url = f"{API_BASE_URL}/config"
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch repository configuration list from API: {e}")
        sys.exit(1)

    try:
        payload = response.json()
    except ValueError as e:
        print(f"Invalid JSON returned from {url}: {e}")
        sys.exit(1)

    endpoints = payload.get("endpoints_configs")
    config = next((c for c in endpoints if c["harvest_url"] == harvest_url), None)

    if config is None:
        print(f"No config found for harvest_url '{harvest_url}'.")
        sys.exit(2)

    print(f"Loaded config for repository '{config.get('code')}' ({harvest_url}).")
    return config


def start_harvest_run(harvest_url: str, timeout: int = 30) -> Optional[dict]:
    """
    POST /harvest_run to create a new harvest run. 
    
    :param harvest_url: endpoint for harvesting
    :return: JSON response (dict) containing at least 'harvest_run_id' and optionally 'last_harvest_date'; return None on error.
    """
    url = f"{API_BASE_URL}/harvest_run"
    payload = {"harvest_url": harvest_url}
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
        run_info = response.json()
        print(f"Started harvest run id={run_info.get('harvest_run_id')}.")
        return run_info
    except requests.RequestException as e:
        print(f"Failed to start harvest run for {harvest_url}: {e}")
        return None


def send_harvest_event(api_base_url, event_payload):
    """
    Send event_payload to API.

    :param api_base_url: API endpoint
    :param event_payload: payload for harvest_event route
    :return logical: True if the payload has been sent to API successfully 
    """
    url = f"{api_base_url}/harvest_event"
    try:
        response = requests.post(url, json=event_payload, timeout=60)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to send record {event_payload["record_identifier"]} to API: {e}")
        return False


def fetch_dataverse_json(doi, base_url, exporter):
    """
    Fetch additional metadata: dataverse json

    :param doi: record identifier
    :param base_url: dataverse API endpoint
    :param exporter: metadata format

    :return: json with additional metadata
    """
    params = {"exporter": exporter, "persistentId": doi}
    try:
        response = requests.get(base_url, params=params, timeout=30)
        if response.status_code == 200:
            return json.dumps(response.json(), indent=2)
        else:
            print(f"Failed to fetch Dataverse JSON for {doi}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching Dataverse JSON for {doi}: {e}")


def fetch_additional_oai(record_id, base_url, metadata_prefix):
    """
    Fetch additional metadata: OAI-PMH

    :param record_id: OAI-PMH record identifier
    :param base_url: OAI-PMH endpoint
    :param metadata_prefix: metadata format

    :return: stringified xml with additional metadata
    """
    try:
        with Scythe(base_url) as client:
            record = client.get_record(identifier=record_id, metadata_prefix=metadata_prefix)
            return ET.tostring(record.xml, pretty_print=True, encoding="unicode")
    except Exception as e:
        print(f"Error fetching {metadata_prefix} metadata for {record_id}: {e}")


def main():
    parser = argparse.ArgumentParser(description="OAI-PMH Harvester (with the possibility of harvesting additional metadata)")
    parser.add_argument("harvest_url", help="Repository OAI-PMH base URL")
    args = parser.parse_args()

    harvest_url = args.harvest_url
    config = load_repo_config(harvest_url)

    # this is an OAI-PMH harvester, exit if it's triggered by a repo with a different primary harvesting protocol
    if config.get("protocol") != "OAI-PMH":
        print(f"Repository '{config["name"]}' skipped: protocol '{config.get("protocol")}' is not supported by this harvester.")
        sys.exit(3)

    # extract harvest parameters from the config
    metadata_prefix = config["harvest_params"].get("metadata_prefix", "oai_dc")
    set = config["harvest_params"].get("set")
    code = config.get("code")
    additional = config.get("additional_metadata_params")
    additional_protocol = additional.get("protocol") if additional else None

    # start new harvest run
    run_info = start_harvest_run(harvest_url)
    if run_info is None:
        sys.exit(1)

    # extract harvest run info from the response
    harvest_run_id = run_info.get("harvest_run_id")
    last_harvest_date = run_info.get("last_harvest_date")

    # harvesting
    harvest_success = False
    try:
        with Scythe(harvest_url) as client:
            if last_harvest_date:
                print(f"Incremental harvest since {last_harvest_date}")
                records = client.list_records(
                    from_=last_harvest_date,
                    metadata_prefix=metadata_prefix,
                    set_=set
                )
            else:
                print("First harvest, fetching all records.")
                records = client.list_records(
                    metadata_prefix=metadata_prefix,
                    set_=set,
                    ignore_deleted=True
                )

            record_count = 0
            harvest_events = 0
            failed_events = 0

            for record in records:
                record_count += 1

                try:
                    identifier = record.header.identifier
                    datestamp = record.header.datestamp
                    is_deleted = getattr(record.header, "status", None) == "deleted"
                    raw_metadata = ET.tostring(record.xml, pretty_print=True, encoding="unicode")

                    additional_metadata = None

                    if not is_deleted:
                        if additional_protocol == "dataverse_api":
                            additional_metadata = fetch_dataverse_json(
                                doi=identifier,
                                base_url=additional["endpoint"],
                                exporter=additional["method"]
                            )

                        elif additional_protocol == "OAI-PMH":
                            additional_metadata = fetch_additional_oai(
                                record_id=identifier,
                                base_url=additional["endpoint"],
                                metadata_prefix=additional["method"]
                            )

                    # metadata and record info to be sent to the warehouse
                    event_payload = {
                        "harvest_run_id": harvest_run_id,
                        "record_identifier": identifier,
                        "datestamp": datestamp,
                        "is_deleted": is_deleted,
                        "raw_metadata": raw_metadata,
                        "additional_metadata": additional_metadata,
                        "harvest_url": harvest_url,
                        "repo_code": code
                    }
                    
                    if send_harvest_event(API_BASE_URL, event_payload):
                        harvest_events += 1
                    else:
                        failed_events += 1

                except Exception as e:
                    failed_events += 1
                    print(f"Record {record_count} failed: {e}")

        harvest_success = True

    except Exception as e:
        print(f"An error occurred during harvesting: {e}")
        traceback.print_exc()

    

if __name__ == "__main__":
    main()
