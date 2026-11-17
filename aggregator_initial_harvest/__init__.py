"""Initial bulk harvest of the Zenodo aggregator.

One-off scripts and helpers that download the Zenodo metadata dump, upload
it to S3 and prepare Apache Iceberg tables for the initial load into the
metadata warehouse. See the ``scripts`` folder for the runnable entry
points, in numbered run order.
"""