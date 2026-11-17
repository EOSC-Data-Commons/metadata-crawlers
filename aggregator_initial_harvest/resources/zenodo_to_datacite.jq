# Transforms a Zenodo record JSON into the DataCite schema
#
# Accepts the full Zenodo record (has .metadata, .links, .files). Needed for urls.
# URLs: We prefer DOI but if it's not available take self_html which is usually a zenodo link

# drop null values and empty arrays so optional fields vanish instead of failing validation
def clean: with_entries(select(.value != null and .value != []));

def is_full_date: type == "string" and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$");

# Strip extra whitespace
def strip: sub("^\\s+"; "") | sub("\\s+$"; "");

. as $rec
| .metadata as $m
| (($m.publication_date // "") | strip) as $pd
| {
    url: ($rec.links.doi
          // (if $rec.pids.doi.identifier then "https://doi.org/" + $rec.pids.doi.identifier else null end)
          // $rec.links.self_html
          ),
    titles:
      ( [ {title: $m.title} ]
        + [ ($m.additional_titles // [])[]
            | {title: .title, titleType: .type.id} ]
      ),

    creators:
      [ ($m.creators // [])[]
        | .person_or_org
        | select(. != null)
        | {
            creatorName: .name,
            nameType: .type,
            givenName: .given_name,
            familyName: .family_name,
            nameIdentifiers:
              [ (.identifiers // [])[]
                | {nameIdentifierScheme: .scheme, nameIdentifier: .identifier}
                | clean ]
          }
        | clean ],
    formats: ([ ($rec.files.entries // {}) | .[] | .mimetype ] | map(select(. != null)) | unique),
    subjects:
      [ ($m.subjects // [])[]
        | {subject: .subject, subjectScheme: .scheme}
        | clean ],

    alternateIdentifiers:
      [ ($m.identifiers // [])[]
        | {alternateIdentifier: .identifier, alternateIdentifierType: .scheme}
        | clean ],

    publicationYear: ( [ (($m.publication_date // "") | strip) | match("^[0-9]{4}") | .string ] | first ),

    descriptions:
      ( [ if $m.description then {description: $m.description, descriptionType: "Abstract"} else empty end ]
        + [ ($m.additional_descriptions // [])[]
            | {description: .description, descriptionType: .type.id} ]
      ),

    dates:
    ( [ if ($pd | is_full_date)
        then {date: $pd, dateType: "Issued"} else empty end ]
      + [ ($m.dates // [])[]
          | select(.date | is_full_date)
          | {date: .date, dateType: (.type.id  // "Other")} ]
    ),

    types: ({
      resourceType: $m.resource_type.title.en,
      resourceTypeGeneral: $m.resource_type.id
    } | clean),

    rightsList:
      [ ($m.rights // [])[]
        | {rights: .title.en, rightsUri: .props.url}
        | clean ]
  }
| clean
