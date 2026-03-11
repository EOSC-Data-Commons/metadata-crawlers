# Repository for metadata crawlers

This repository contains a Python package for metadata crawling and ingestion.
Currently, it supports harvesting metadata via the OAI-PMH protocol, using the oaipmh-scythe Python client library, and a specialized harvester for FinBIF repository.
The package is containerized with Docker and can be run locally or deployed as part of a larger metadata ingestion system.
This is work in progress and will be extended with additional harvesting methods in the future.

## Overview

The crawler is designed as a modular Python package.
It performs the following tasks:
1. Starts a new harvest run in the database via an API call to /harvest_run.
2. Retrieves configuration info for the given repository endpoint.
3. Determines the harvesting protocol (currently only OAI-PMH and FinBIF API are supported) and executes the appropriate harvesting module.
4. Sends harvested metadata to the database via an API call to /harvest_event.
5. Closes the harvest run with a success or failure status.
6. Logs all stages of the workflow to both console and rotating log files.

## Architecture
The package consists of the following components:
- main.py - entry point that namages harvest runs, fetches config info and runs the appropriate harvesting module
- db_api_functions.py - contains functions used to communicate with the database API
- harvester_oaipmh.py - harvesting module for repositories that expose metadata via OAI-PMH
- harvester_finbif.py - harvesting module for FinBIF repository
- ddi_to_datacite.xsl - XSLT stylesheet that transforms metadata format from DDI to DataCite format
- finbif_to_datacite.xsl - XSLT stylesheet that transforms metadata format from FinBIF dictionary to DataCite format
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
FINBIF_ACCESS_TOKEN=
```
- ```ENVIRONMENT``` chooses a configuration profile (local, dev, staging, production)
- ```WAREHOUSE_API_URL``` must point to the Warehouse API instance the harvester will send results to.
For Docker execution, the ```WAREHOUSE_API_URL``` can be set in ```docker-compose.yml```.
- To use FinBIF API you need to use an access token ```FINBIF_ACCESS_TOKEN``` - see https://info.laji.fi/en/frontpage/api/api-laji-fi/

## Running Locally
To run the harvester directly on your machine:
1. Ensure ```.env``` exists in the project root and the required values have been filled
2. Run the following:
```
python -m harvester {repository URL}
```
Replace ```{repository URL}``` with the actual base URL of the repository you want to harvest.

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

### Add a new harvest protocol

> [!WARNING]
>
> The metadata-warehouse database needs to be running and initialized:
>
> ```sh
> cd metadata-warehouse
> docker compose up -d
> cd scripts/postgres_data
> uv run create_db.py --db datasetdb --reset
> ```

1. Update the `harvest_protocol` enum created in [`types.sql`](https://github.com/EOSC-Data-Commons/metadata-warehouse/blob/main/scripts/postgres_data/create_sql/datasetdb/types.sql):

   ```sql
   ALTER TYPE harvest_protocol ADD VALUE 'SCHEMAORG_EMBEDDED_JSONLD';
   ```

2. Add an endpoint for your new harvest protocol to the `endpoints` table in the [metadata-warehouse](https://github.com/EOSC-Data-Commons/metadata-warehouse) SQL database, for now it is done in the [`seed.sql` script](https://github.com/EOSC-Data-Commons/metadata-warehouse/blob/main/scripts/postgres_data/create_sql/datasetdb/seed.sql), e.g.

   ```sql
   INSERT INTO endpoints (repository_id, name, harvest_url, protocol, scientific_discipline, is_active, harvest_params)
   SELECT
       r.id,
       'Bgee',
       'https://www.bgee.org/search/species',
       'SCHEMAORG_EMBEDDED_JSONLD',
       'Biology',
       true,
       '{"metadata_prefix": "schema", "index_parent_dataset": true}'
   FROM repositories r
   WHERE r.code = 'SWISS'
   ON CONFLICT (name) DO NOTHING;
   ```

3. Create a file in `harvester/harvesters/` folder and add your harvest function

4. Update the `main.py` file to add your harvest function for your new protocol

5. Run your harvester:

   ```sh
   uv run harvester https://www.bgee.org/search/species
   ```

## Update DataCite model

The pydantic model to easily create DataCite records is automatically generated from the [JSON schema used by the metadata-warehouse](https://github.com/EOSC-Data-Commons/metadata-warehouse/blob/main/src/config/schema.json), to update it run:

```sh
uvx --from "datamodel-code-generator[http]" datamodel-codegen \
  --url "https://raw.githubusercontent.com/EOSC-Data-Commons/metadata-warehouse/refs/heads/main/src/config/schema.json" \
  --output harvester/datacite_model.py --input-file-type jsonschema \
  --output-model-type pydantic_v2.BaseModel --use-annotated \
  --class-name DataciteRecord
```

## License

This project is licensed under the Apache License 2.0.
See the LICENSE file for details.

This project uses the [oaipmh-scythe](https://github.com/afuetterer/oaipmh-scythe) Python client,
which is distributed under the BSD license.
The BSD license is a permissive open source license that allows use, modification, and distribution.
For full license details, see the [oaipmh-scythe license](https://github.com/afuetterer/oaipmh-scythe/blob/master/LICENSE).
