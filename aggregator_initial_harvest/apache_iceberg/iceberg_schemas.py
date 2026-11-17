from pyiceberg.schema import Schema
from pyiceberg.types import DateType, NestedField, StringType

# Schemas for Iceberg tables

# Table 1 - all records
SCHEMA_AGGREGATOR_RECORDS = Schema(
    NestedField(field_id=1, name="file", field_type=StringType(), required=True),
    NestedField(field_id=2, name="id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="source", field_type=StringType(), required=True),
    NestedField(field_id=4, name="resource_type", field_type=StringType(), required=False),
    NestedField(field_id=5, name="created", field_type=DateType(), required=False),
    NestedField(field_id=6, name="updated", field_type=DateType(), required=False),
)

# Table 2 - records to turn into harvest_events. Contain raw JSON and transformed DataCite JSON
SCHEMA_RECORDS_TO_HARVEST = Schema(
    NestedField(field_id=1, name="file", field_type=StringType(), required=True),
    NestedField(field_id=2, name="id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="source", field_type=StringType(), required=True),
    NestedField(field_id=4, name="resource_type", field_type=StringType(), required=False),
    NestedField(field_id=5, name="created", field_type=DateType(), required=False),
    NestedField(field_id=6, name="updated", field_type=DateType(), required=False),
    NestedField(field_id=7, name="raw_json", field_type=StringType(), required=False),
    # raw -> DataCite JSON
    NestedField(field_id=8, name="datacite_json", field_type=StringType(), required=False),
    # raw -> zenodo files schema JSON (left empty for now)
    NestedField(field_id=9, name="additional_metadata_json", field_type=StringType(), required=False),
)