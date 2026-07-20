import unittest
import json
from harvester.harvester_finbif import build_datacite_xml, filter_datasets_by_date
from datetime import datetime

class TestTransformationAndAdditionalMetadata(unittest.TestCase):
    def test_build_datacite_xml(self):

        with open("tests/testdata/finbif/finbif_combined.json") as f:
            finbif_json = json.load(f)

        datacite_xml = build_datacite_xml(finbif_json)

        with open("tests/testdata/finbif/finbif_datacite.xml") as f:
            expected = f.read()

        self.assertEqual(datacite_xml, expected)


    def test_filter_datasets_by_date_later(self):
        from_date = datetime.fromisoformat("2026-05-21T00:00:00+00:00")
        result = filter_datasets_by_date([{"doi": "10.15468/vcddkt", "modified": "2026-05-22T00:05:05.784000+00:00"},], from_date)

        self.assertEqual(len(result), 1)

    def test_filter_datasets_by_date_earlier(self):
        from_date = datetime.fromisoformat("2026-05-23T00:00:00+00:00")
        result = filter_datasets_by_date([{"doi": "10.15468/vcddkt", "modified": "2026-05-22T00:05:05.784000+00:00"}, ],
                                         from_date)

        self.assertEqual(len(result), 0)
