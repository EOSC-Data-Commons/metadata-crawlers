import unittest
from unittest.mock import patch, Mock
from pathlib import Path
from harvester.harvester_oaipmh import transformation_and_additional_metadata
from lxml import etree
import json

def load_xml(filename):
    path = Path(__file__).parent / "testdata" / filename
    return path.read_text(encoding="utf-8")


def normalize_xml(xml_string):
    parser = etree.XMLParser(remove_blank_text=True)
    root = etree.fromstring(xml_string.encode("utf-8"), parser)

    return etree.tostring(
        root,
        pretty_print=False,
        encoding="unicode"
    )

def load_json(filename):
    with open(Path(__file__).parent / "testdata" / filename, "r", encoding="utf-8") as f:
        return json.load(f)

class TestTransformationAndAdditionalMetadata(unittest.TestCase):

    # oai_dc -> oai_datacite
    # no additional metadata
    def test_transformation_success_panosc_oai_dc(self):
        result = transformation_and_additional_metadata(
            raw_metadata=load_xml("elletra_raw.xml"),
            metadata_prefix="oai_dc",
            identifier="9442",
            additional_protocol=None,
            additional_endpoint=None,
            additional_format=None
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("elletra_transformed.xml")))
        self.assertEqual(normalize_xml(result[1]), normalize_xml(load_xml("elletra_raw.xml")))

    # oai_datacite
    # no additional metadata
    def test_transformation_success_panosc_oai_datacite(self):
        result = transformation_and_additional_metadata(
            raw_metadata=load_xml("hzdr_raw.xml"),
            metadata_prefix="oai_datacite",
            identifier="oai:rodare.hzdr.de:970",
            additional_protocol=None,
            additional_endpoint=None,
            additional_format=None
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("hzdr_raw.xml")))
        self.assertEqual(result[1], None)


    # oai_datacite
    # additional_metadata dataverse_json
    @patch("harvester.harvester_oaipmh.fetch_dataverse_json")
    def test_transformation_success_dans(self, mock_transform):
        mock_transform.return_value = load_json("dans_additional.json")
        result = transformation_and_additional_metadata(
            raw_metadata=load_xml("dans_raw.xml"),
            metadata_prefix="oai_datacite",
            identifier="doi:10.17026/AR/019KSM",
            additional_protocol="DATAVERSE_API",
            additional_endpoint="https://archaeology.datastations.nl/api/datasets/:persistentId/versions/:latest-published",
            additional_format="dataverse_json"
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("dans_raw.xml")))
        self.assertEqual(result[1], load_json("dans_additional.json"))


    # oai_ddi25 -> oai_datacite
    # no additional_metadata
    def test_transformation_success_swissubase(self):
        result = transformation_and_additional_metadata(
            raw_metadata=load_xml("swissubase_raw.xml"),
            metadata_prefix="oai_ddi25",
            identifier="oai:swissubase.ch:0bc01797-aef3-4fda-953b-91edb56852d9",
            additional_protocol=None,
            additional_endpoint=None,
            additional_format=None
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("swissubase_transformed.xml")))
        self.assertEqual(normalize_xml(result[1]), normalize_xml(load_xml("swissubase_raw.xml")))


    # oai_datacite
    # additional metadata oai-pmh
    @patch("harvester.harvester_oaipmh.fetch_additional_oai")
    def test_transformation_success_dabar(self, mock_transform):
        mock_transform.return_value = load_xml("dabar_additional.xml")
        result = transformation_and_additional_metadata(
            raw_metadata=load_xml("dabar_raw.xml"),
            metadata_prefix="oai_datacite",
            identifier="oai:dabar.srce.hr:irb_5",
            additional_protocol="OAI-PMH",
            additional_endpoint="https://dabar.srce.hr/oai/",
            additional_format="mods"
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("dabar_raw.xml")))
        self.assertEqual(normalize_xml(result[1]), normalize_xml(load_xml("dabar_additional.xml")))