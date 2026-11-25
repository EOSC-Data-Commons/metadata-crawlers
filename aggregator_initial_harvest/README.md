# Apache Iceberg configuration and scripts

Currently used to setup an initial harvest of Zenodo metadata.  

## Scripts  

Each script is meant to be ran in sequence.

### Push Zenodo metadata dump to S3
Fills a S3 bucket with the contents of the latest Zenodo metadata dump.

To run the script, execute the following:   
`uv run aggregator_initial_harvest/scripts/01_zenodo_dump_to_s3.py --bucket test_zenodo_dump`  
You can add `--cleanup-local-dump yes` to delete the ~13GB dump that gets downloaded after the script finishes.

It goes through the following steps:
* creates the `--bucket` bucket if it doesn't already exist on the S3
* finds the latest JSON metadata dump from Zenodo
* downloads the dump to local drive
* uploads the exporter-API info of that dump to the bucket as `dump_metadata/dump_metadata.json`, so the dump's creation date can be queried later
* uses parallel workers to upload the records from the dump to the specified bucket, with the "zenodo_dump/" prefix
* downloads the latest `records-deleted.csv.gz` from the exporter API and deletes the bucket objects of every record it lists (`zenodo_dump/<record_id>.json`), so records removed by Zenodo don't linger in the bucket. Deleting a key that isn't in the bucket is a no-op, so the perfomance impact shouldn't be large
* if any records raised an error during upload, the `<bucket>_errors.log` file will be created listing the record ID and the specific error raised

Note: Downloading the .tar.gz dump requires about 13GB of space and might take minutes. Uploading the millions of records may take **hours**!

### Fill the aggregator_records Iceberg table
Drops and rebuilds the `aggregator_records` table defined in `src/apache_iceberg/iceberg_schemas.py` in the given `--namespace`: one row for each `*.json` record under the `--prefix`, with id, resource type and dates, so later steps can select which records to harvest.

To run the script, execute the following:  
`uv run aggregator_initial_harvest/scripts/02_fill_iceberg_aggregator_records_zenodo.py --bucket zenodo20k-2026-10-03 --namespace test_harvest_ice`

It goes through the following steps:
* connects to the Iceberg REST catalog and the S3 endpoint, with credentials from `.env`
* creates the `--namespace` namespace if it doesn't already exist
* rebuilds `aggregator_records` from every `*.json` object under the `--prefix`, committing in slices of `--slice` rows
* records that fail to read or parse are logged and skipped so the run doesn't fail in the middle

Mandatory arguments:  
`--bucket` : name of s3 bucket where the dump is stored. The s3 credentials are provided in ENV  
`--namespace` : Iceberg namespace where the table will live. Created if nonexistant

Optional arguments:  
`--prefix` : Folder in the `--bucket`. Default "zenodo_dump"  
`--slice` : Number of records per commit. Default 10000  
`--workers` : For working with S3 in parallel. Default 32

### Fill the records_to_harvest Iceberg  table
Drops and rebuilds the `records_to_harvest` table defined in `src/apache_iceberg/iceberg_schemas.py` in the given `--namespace`.
This table is a subset of the *aggregator_records* table, currently only those records with `resource_type = "dataset"`.
For these records their full raw_JSON is stored, as well as its DataCite and additional_metadata transforms. The JSON
transformations will be used to create *harvest_events* in the metadata warehouse.

To run the script, execute the following:  
`uv run aggregator_initial_harvest/scripts/03_fill_iceberg_records_to_harvest_zenodo.py --bucket zenodo20k-2026-10-03 --namespace test_harvest_ice`

It goes through the following steps:
* connects to the Iceberg REST catalog and the S3 endpoint, with credentials from `.env`
* selects the dataset records from `aggregator_records` (before dropping anything, so a missing table 1 fails the run instead of emptying table 2)
* rebuilds `records_to_harvest`, reading each selected record from S3 again and storing it with the raw JSON, the DataCite JSON and the additional_metadata JSON
* records that fail to read or parse are logged and skipped so the run doesn't fail in the middle

Mandatory arguments:  
`--bucket` : name of s3 bucket where the dump is stored. The s3 credentials are provided in ENV  
`--namespace` : Iceberg namespace where the tables live

Optional arguments:  
`--slice` : Number of records per commit. Default 5000  
`--workers` : For working with S3 in parallel. Default 32



### Use Iceberg records as "harvest_events" in ***Metadata Warehouse***
TODO  
? In which repository should this script live?
