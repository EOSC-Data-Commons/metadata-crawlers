# Repository for metadata crawlers

This repository contains a Python package for metadata crawling and ingestion.
Currently, it supports harvesting metadata via the OAI-PMH protocol, using the oaipmh-scythe Python client library.
The package is containerized with Docker and can be run locally or deployed as part of a larger metadata ingestion system.
This is work in progress and will be extended with additional harvesting methods in the future.

## Overview

The crawler is designed as a modular Python package.
It performs the following tasks:
1. Starts a new harvest run in the database via an API call to /harvest_run.
2. Retrieves configuration info for the given repository endpoint.
3. Determines the harvesting protocol (currently only OAI-PMH is supported) and executes the appropriate harvesting module.
4. Sends harvested metadata to the database via an API call to /harvest_event.
5. Closes the harvest run with a success or failure status.
6. Logs all stages of the workflow to both console and rotating log files.

## Architecture
The package consists of the following components:
- main.py - entry point that namages harvest runs, fetches config info and runs the appropriate harvesting module
- db_api_functions.py - contains functions used to communicate with the database API 
- harvester_oaipmh.py - harvesting module for repositories that expose metadata via OAI-PMH
- ddi_to_datacite.xsl - XSLT stylesheet that transforms metadata format from DDI to DataCite format
- logging.py - logging config function

## Requirements
- [Python](https://www.python.org/downloads/) >= 3.10
- Dependencies listed in requirements.txt 

## Running Locally
To run the harvester directly on your machine:
```
python -m harvester {repository URL}
```
Replace {repository URL} with the actual OAI-PMH base URL of the repository you want to harvest.

The harvester uses environment variables for configuration, including the base URL of the database API.
These variables can be provided via a .env file in the project root or manually exported before execution.

## Docker Usage
The repository includes a Dockerfile and a docker-compose.yaml.
These can be used to build and run the harvester in an isolated environment.

Build the image:
```
docker compose build
```
Run the harvester once:
```
docker compose run --rm harvester {repository URL}
```

## Logs and Output
The harvester does not write harvested metadata to disk.
Instead, all records and harvest run status updates are sent directly to the database API.

Logs include:
- Informational messages about harvesting progress
- Warnings about potentially problematic responses
- Errors indicating failed requests, invalid responses, or failed harvest runs

Logs are saved to:
``` /app/logs/harvester.log ```

When running in Docker, this log directory can be mounted as a volume to persist logs outside the container.

## Future Extensions
Future versions of this package will support additional crawling protocols.

## License
This project uses the [oaipmh-scythe](https://github.com/afuetterer/oaipmh-scythe) Python client,  
which is distributed under the BSD license.

The BSD license is a permissive open source license that allows use, modification, and distribution.  
For full license details, see the [oaipmh-scythe license](https://github.com/afuetterer/oaipmh-scythe/blob/master/LICENSE).