import unittest
import json
from harvester.harvester_finbif import build_datacite_xml

class TestTransformationAndAdditionalMetadata(unittest.TestCase):
    def test_build_datacite_xml(self):

        with open("tests/testdata/finbif/finbif_combined.json") as f:
            finbif_json = json.load(f)

        datacite_xml = build_datacite_xml(finbif_json)

        with open("tests/testdata/finbif/finbif_datacite.xml") as f:
            expected = f.read()

        self.assertEqual(datacite_xml, expected)