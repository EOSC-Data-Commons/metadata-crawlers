# script for harvesting metadata based on oaipmh-scythe client
# runs with repository endpoint as argument: python harvester-oaipmh.py {repository_url}

import sys
import os
import argparse
from datetime import datetime, timezone
from lxml import etree as ET
import json
from oaipmh_scythe import Scythe
import requests
import traceback
from typing import Dict, Optional, Any
import time

NS = {"oai": "http://www.openarchives.org/OAI/2.0/"}
API_BASE_URL = "http://127.0.0.1:8080"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def start_harvest_run(harvest_url: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    """
    POST /harvest_run to create a new harvest run. 
    
    :param harvest_url: endpoint for harvesting
    :param timeout: Request timeout in seconds
    :return: JSON response (dict) containing 'harvest_run_id', optionally 'last_harvest_date', and endpoint config; returns None on error.
    """
    url = f"{API_BASE_URL}/harvest_run"
    payload = {"harvest_url": harvest_url}
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
        run_info = response.json()
        print(f"Started harvest run id={run_info.get('id')}.")
        return run_info
    except requests.RequestException as e:
        print(f"Failed to start harvest run for {harvest_url}: {e}")
        return None

def get_open_run_id(harvest_url: str, timeout: int = 30) -> Optional[Dict]:
    """
    GET /harvest_run to fetch an open harvest run ID if it exists.

    :param harvest_url: endpoint for harvesting
    :param timeout: Request timeout in seconds
    :return: JSON response (dict) containing the ID of a harvest run and its status, or None if not found or failed
    """
    url = f"{API_BASE_URL}/harvest_run"
    params = {"harvest_url": harvest_url}
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()

        response = response.json()
        if response["status"] == "open":
            return response["id"]
        else:
            return None

    except requests.RequestException as e:
        print(f"Error checking for open harvest run for {harvest_url}: {e}")
        return None

def close_harvest_run(api_base_url: str, payload: Dict) -> None:
    """
    PUT /harvest_run to close the harvest run.

    :param api_base_url: API endpoint
    :param payload: payload for API post request to close the harvest run
    """
    url = f"{api_base_url}/harvest_run"
    try:
        response = requests.put(url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"Closed harvest run {payload["id"]} — started {payload["started_at"]}, finished {payload["completed_at"]}")
    except requests.RequestException as e:
        if e.response is not None:
            print(f"Failed to close harvest run {payload["id"]}: API error: {e.response.status_code} {e.response.text}")
        else:
            print(f"Failed to close harvest run {payload["id"]}: {e}")


def send_harvest_event(api_base_url: str, event_payload: Dict) -> bool:
    """
    Send event_payload to API.

    :param api_base_url: API endpoint
    :param event_payload: dictionary containing event data for harvest_event route
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


def fetch_dataverse_json(doi: str, base_url: str, exporter: str) -> Optional[str]:
    """
    Fetch additional metadata: dataverse json

    :param doi: record identifier
    :param base_url: dataverse API endpoint
    :param exporter: exporter type
    :return: stringified JSON with additional metadata; returns None on error
    """
    params = {"exporter": exporter, "persistentId": doi}
    try:
        response = requests.get(base_url, params=params, timeout=30)
        if response.status_code == 200:
            return json.dumps(response.json(), indent=2)
        else:
            print(f"Failed to fetch Dataverse JSON for {doi}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching Dataverse JSON for {doi}: {e}")
        return None


def fetch_additional_oai(record_id: str, base_url: str, metadata_prefix: str) -> Optional[str]:
    """
    Fetch additional metadata: OAI-PMH

    :param record_id: OAI-PMH record identifier
    :param base_url: OAI-PMH endpoint
    :param metadata_prefix: metadata format
    :return: stringified XML with additional metadata; returns None on error
    """
    try:
        with Scythe(base_url) as client:
            record = client.get_record(identifier=record_id, metadata_prefix=metadata_prefix)
            return ET.tostring(record.xml, pretty_print=True, encoding="unicode")
    except Exception as e:
        print(f"Error fetching {metadata_prefix} metadata for {record_id}: {e}")
        return None


def apply_xslt_transform(ddi_xml: str, transform: ET.XSLT) -> str | None:
    """
    Apply a precompiled XSLT transform to a DDI XML string. (for SwissUBase)

    :param ddi_xml: DDI XML as string
    :param transform: Compiled lxml.etree.XSLT object
    :return: Transformed XML as string, or None on failure
    """
    try:
        ddi_doc = ET.fromstring(ddi_xml.encode("utf-8"))
        result_tree = transform(ddi_doc)
        return ET.tostring(result_tree, pretty_print=True, encoding="UTF-8").decode("utf-8")
    except Exception as e:
        print(f"Transformation failed: {e}")
        return None



def main():
    parser = argparse.ArgumentParser(description="OAI-PMH Harvester (with the possibility of harvesting additional metadata)")
    parser.add_argument("harvest_url", help="Repository OAI-PMH base URL")
    args = parser.parse_args()

    harvest_url = args.harvest_url

    # start new harvest run
    run_info = start_harvest_run(harvest_url)
    start_time = datetime.now(timezone.utc).isoformat(timespec='seconds')

    # if there is no response, try closing it
    if run_info is None:
        print("Failed to start harvest run, checking for existing open run...")
        open_run_id = get_open_run_id(harvest_url)  
        if open_run_id:
            print(f"Closing harvest run {open_run_id} with status 'failed'")
            end_time = datetime.now(timezone.utc).isoformat(timespec='seconds')
            close_harvest_run_payload = {
                "id": open_run_id,
                "success": False,
                "started_at": start_time,
                "completed_at": end_time
                }
            close_harvest_run(API_BASE_URL, close_harvest_run_payload)
        sys.exit(1)
    
    
    harvest_run_id = run_info.get("id")
    harvest_success = False
    try:
        # setup for harvest
        # extract config from the response
        from_date = run_info.get("from_date")
        config = run_info.get("endpoint_config")
        if not config:
            raise ValueError("Missing endpoint configuration in run_info")

        # this is an OAI-PMH harvester, raise error if it's triggered by a repo with a different primary harvesting protocol
        if config.get("protocol") != "OAI-PMH":
            raise ValueError(f"Protocol {config.get('protocol')} is not supported by this harvester")

        # extract harvest parameters from the config
        harvest_params = config.get("harvest_params")
        metadata_prefix = harvest_params.get("metadata_prefix", "oai_dc")
        set = harvest_params.get("set")
        code = config.get("code")
        additional = harvest_params.get("additional_metadata_params")
        additional_protocol = additional.get("protocol") if additional else None

        # if schema is not DataCite, we will need to transform the XML
        if metadata_prefix == "oai_ddi25":
            XSLT_PATH = os.path.join(BASE_DIR, "ddi_to_datacite.xsl")
            xslt_doc = ET.parse(XSLT_PATH)
            transform = ET.XSLT(xslt_doc)

        # harvesting
        with Scythe(harvest_url, timeout=180, max_retries=3, default_retry_after=60) as client:
            if from_date:
                print(f"Incremental harvest since {from_date}")
                records = client.list_records(
                    from_=from_date,
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
                        if additional_protocol == "REST_API":
                            additional_metadata = fetch_dataverse_json(
                                doi=identifier,
                                base_url=additional["endpoint"],
                                exporter=additional["format"]
                            )

                        elif additional_protocol == "OAI-PMH":
                            additional_metadata = fetch_additional_oai(
                                record_id=identifier,
                                base_url=additional["endpoint"],
                                metadata_prefix=additional["format"]
                            )

                        elif metadata_prefix == "oai_ddi25":
                            additional_metadata = raw_metadata
                            raw_metadata = apply_xslt_transform(raw_metadata, transform)
                            if raw_metadata is None:
                                print(f"Skipping record {identifier}: transformation to DataCite failed.")
                                failed_events += 1
                                continue

                    # metadata and record info to be sent to the warehouse
                    event_payload = {
                        "record_identifier": identifier,
                        "datestamp": datestamp,
                        "raw_metadata": raw_metadata,
                        "additional_metadata": additional_metadata,
                        "harvest_url": harvest_url,
                        "repo_code": code,                          # do we need this?
                        "harvest_run_id": harvest_run_id,
                        "is_deleted": is_deleted
                    }
                    
                    if send_harvest_event(API_BASE_URL, event_payload):
                        harvest_events += 1
                    else:
                        failed_events += 1

                except Exception as e:
                    failed_events += 1
                    print(f"Record {record_count} failed: {e}")

                if record_count % 100 == 0:
                    time.sleep(1)

        if failed_events == 0:
            harvest_success = True

    except Exception as e:
        print(f"An error occurred during harvesting: {e}")
        traceback.print_exc()

    finally:
        print(f"Harvested {record_count} records. Succesfully sent {harvest_events} of them to the warehouse. Failed to upload {failed_events} records.")
        end_time = datetime.now(timezone.utc).isoformat(timespec='seconds')
        close_harvest_run_payload = {
            "id": harvest_run_id,
            "success": harvest_success,
            "started_at": start_time,
            "completed_at": end_time
        }
        close_harvest_run(API_BASE_URL, close_harvest_run_payload)

if __name__ == "__main__":
    main()
