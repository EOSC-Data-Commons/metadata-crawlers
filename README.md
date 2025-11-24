# Repository for metadata crawlers

Python package for metadata crawling and ingestion.
Currently the package only supports harvesting metadata via OAI-PMH, based on the oaipmh-scythe Python client. 
Contains Docker support and environment variable configuration for easier deployment.
This is work in progress.

## Architecture
The package consists of the following modules:
- main.py - entry point that namages harvest runs, fetches config info and runs the appropriate harvesting module
- db_api_functions.py - contains functions for communication with the database API 
- harvester_oaipmh.py - harvesting module for repositories that expose metadata via OAI-PMH
- ddi_to_datacite.xsl - metadata format transformer
- logging.py - logging config function

## Requirements
- [Python](https://www.python.org/downloads/) >= 3.10
- see requirements.txt 

## Usage
From the command line run:
```sh
python -m harvester {repository URL}
```
Replace {repository URL} with the actual OAI-PMH endpoint of the repository you want to harvest from.

## Output
Results are sent to the database API. Logs are saved in a log file. 

## License
This project uses the [oaipmh-scythe](https://github.com/afuetterer/oaipmh-scythe) Python client,  
which is distributed under the BSD license.

The BSD license is a permissive open source license that allows use, modification, and distribution.  
For full license details, see the [oaipmh-scythe license](https://github.com/afuetterer/oaipmh-scythe/blob/master/LICENSE).