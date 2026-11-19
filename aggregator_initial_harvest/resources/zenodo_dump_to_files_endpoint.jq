# Transforms a Zenodo record JSON into the format provided by
# the /files endpoint, which we need for additional metadata

. as $rec
| ($rec.links.files // ($rec.links.self + "/files")) as $files_url
| {
    enabled: ($rec.files.enabled // true),
    order: ($rec.files.order // []),
    links: { self: $files_url },
    entries: [
      ($rec.files.entries // {})
      | to_entries[]
      | .key as $name
      | .value as $f
      | ($f.key // $name) as $key
      | {
          # These datetimes are copied from the record itself for each file.
          created: $rec.created,
          updated: $rec.updated,
          version_id: $f.id,

          file_id: $f.id,
          key: $key,
          size: $f.size,
          checksum: $f.checksum,
          mimetype: $f.mimetype,
          metadata: ($f.metadata // {}),
          access: ($f.access // {hidden: false}),
          links: {
            self: ($files_url + "/" + ($key | @uri)),
            content: ($files_url + "/" + ($key | @uri) + "/content")
          }
        }
      # drop optional fields that are missing in the source (null)
      | with_entries(select(.value != null))
    ]
  }
