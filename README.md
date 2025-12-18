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
- settings.py - handles settings for different environments

## Requirements
- [Python](https://www.python.org/downloads/) >= 3.10
- Dependencies listed in requirements.txt 

## Environment configuration
The harvester uses Pydantic settings for all configuration.
All configuration values come from:

1. Docker environment variables (if running inside Docker)
2. A local .env file (if running on your machine)
3. Internal defaults in settings.py

You must create a .env file before running the harvester locally.

To do this:
copy the example file ```cp .env.example .env```
and fill in the required values:
```
ENVIRONMENT=local
WAREHOUSE_API_URL=http://localhost:8000
```
- ```ENVIRONMENT``` chooses a configuration profile (local, dev, staging, production)
- ```WAREHOUSE_API_URL``` must point to the Warehouse API instance the harvester will send results to
For Docker execution, the ```WAREHOUSE_API_URL``` can be set in ```docker-compose.yml```.

## Running Locally
To run the harvester directly on your machine:
1. Ensure ```.env``` exists in the project root and the required values have been filled
2. Run the following:
```
python -m harvester {repository URL}
```
Replace ```{repository URL}``` with the actual OAI-PMH base URL of the repository you want to harvest.

## Docker Usage
The repository includes a ```Dockerfile``` and a ```docker-compose.yml```.
These can be used to build and run the harvester in an isolated environment.

Build the image:
```
docker compose build
```
Run the harvester once:
```
docker compose run --rm harvester {repository URL}
```

By default, ```.env``` is loaded in ```docker-compose.yml```, but you can replace that with appropriate values for ```ENVIRONMENT``` and ```WAREHOUSE_API_URL```.

## Logs and Output
The harvester does not write harvested metadata to disk.
Instead, all records and harvest run status updates are sent directly to the database API.

Logs include:
- Informational messages about harvesting progress
- Warnings about potentially problematic responses
- Errors indicating failed requests, invalid responses, or failed harvest runs

Logs are saved to:
``` logs/harvester.log ```

When running in Docker, this log directory can be mounted as a volume to persist logs outside the container.

## Future Extensions
Future versions of this package will support additional crawling protocols.

## License
This project uses the [oaipmh-scythe](https://github.com/afuetterer/oaipmh-scythe) Python client,  
which is distributed under the BSD license.

The BSD license is a permissive open source license that allows use, modification, and distribution.  
For full license details, see the [oaipmh-scythe license](https://github.com/afuetterer/oaipmh-scythe/blob/master/LICENSE).