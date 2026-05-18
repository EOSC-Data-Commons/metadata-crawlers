import unittest, json
from unittest.mock import patch, Mock
from pathlib import Path
from lxml import etree as ET
from harvester.harvester_oaipmh import transformation_and_additional_metadata, run_harvester_oaipmh


# -----------------------------
# Test helpers (file loading & normalization)
# -----------------------------
def load_xml(filename):
    path = Path(__file__).parent / "testdata" / filename
    return path.read_text(encoding = "utf-8")


def normalize_xml(xml_string):
    # Used to compare XML outputs without formatting differences
    parser = ET.XMLParser(remove_blank_text = True)
    root = ET.fromstring(xml_string.encode("utf-8"), parser)

    return ET.tostring(root, pretty_print = False, encoding = "unicode")


def load_json(filename):
    with open(Path(__file__).parent / "testdata" / filename, "r", encoding = "utf-8") as f:
        return json.load(f)


# -----------------------------
# Unit tests: transformation_and_additional_metadata
# -----------------------------
class TestTransformationAndAdditionalMetadata(unittest.TestCase):

    # Case 1: Basic DataCite input should pass through unchanged
    def test_transformation_basic_oai_datacite(self):
        result = transformation_and_additional_metadata(
            raw_metadata = "<basic>",
            metadata_prefix = "oai_datacite",
            identifier = "123",
            additional_protocol = None,
            additional_endpoint = None,
            additional_format = None
        )
        self.assertEqual(result, ("<basic>", None))


    # Case 2: Real DataCite input should pass through unchanged
    def test_transformation_success_panosc_oai_datacite(self):
        result = transformation_and_additional_metadata(
            raw_metadata = load_xml("hzdr_raw.xml"),
            metadata_prefix = "oai_datacite",
            identifier = "oai:rodare.hzdr.de:970",
            additional_protocol = None,
            additional_endpoint = None,
            additional_format = None
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("hzdr_raw.xml")))
        self.assertEqual(result[1], None)


    # Case 3: OAI_DC -> DataCite transformation (no additional metadata)
    def test_transformation_success_panosc_oai_dc(self):
        result = transformation_and_additional_metadata(
            raw_metadata = load_xml("elletra_raw.xml"),
            metadata_prefix = "oai_dc",
            identifier = "9442",
            additional_protocol = None,
            additional_endpoint = None,
            additional_format = None
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("elletra_transformed.xml")))
        self.assertEqual(normalize_xml(result[1]), normalize_xml(load_xml("elletra_raw.xml")))


    # Case 4: OAI_DDI25 -> DataCite transformation (no additional metadata)
    def test_transformation_success_swissubase(self):
        result = transformation_and_additional_metadata(
            raw_metadata = load_xml("swissubase_raw.xml"),
            metadata_prefix = "oai_ddi25",
            identifier = "oai:swissubase.ch:0bc01797-aef3-4fda-953b-91edb56852d9",
            additional_protocol = None,
            additional_endpoint = None,
            additional_format = None
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("swissubase_transformed.xml")))
        self.assertEqual(normalize_xml(result[1]), normalize_xml(load_xml("swissubase_raw.xml")))


    # Case 5: DataCite + Dataverse additional metadata
    @patch("harvester.harvester_oaipmh.fetch_dataverse_json")
    def test_transformation_success_dans(self, mock_transform):
        mock_transform.return_value = load_json("dans_additional.json")
        result = transformation_and_additional_metadata(
            raw_metadata = load_xml("dans_raw.xml"),
            metadata_prefix = "oai_datacite",
            identifier = "doi:10.17026/AR/019KSM",
            additional_protocol = "DATAVERSE_API",
            additional_endpoint = "https://archaeology.datastations.nl/api/datasets/:persistentId/versions/:latest-published",
            additional_format = "dataverse_json"
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("dans_raw.xml")))
        self.assertEqual(result[1], load_json("dans_additional.json"))


    # Case 6: DataCite + OAI-PMH additional metadata
    @patch("harvester.harvester_oaipmh.fetch_additional_oai")
    def test_transformation_success_dabar(self, mock_transform):
        mock_transform.return_value = load_xml("dabar_additional.xml")
        result = transformation_and_additional_metadata(
            raw_metadata = load_xml("dabar_raw.xml"),
            metadata_prefix = "oai_datacite",
            identifier = "oai:dabar.srce.hr:irb_5",
            additional_protocol = "OAI-PMH",
            additional_endpoint = "https://dabar.srce.hr/oai/",
            additional_format = "mods"
        )
        self.assertEqual(normalize_xml(result[0]), normalize_xml(load_xml("dabar_raw.xml")))
        self.assertEqual(normalize_xml(result[1]), normalize_xml(load_xml("dabar_additional.xml")))


    # Case 7: Invalid XML for OAI_DC -> DataCite transformation
    def test_transformation_invalid_xml_returns_none(self):
        result = transformation_and_additional_metadata(
            raw_metadata = "<invalid>",
            metadata_prefix = "oai_dc",
            identifier = "123",
            additional_protocol = None,
            additional_endpoint = None,
            additional_format = None
        )
        self.assertEqual(result, (None, None))


# -----------------------------
# Unit tests: run_harvester_oaipmh (integration-style tests)
# These tests simulate a full harvesting pipeline using mocks:
# - Scythe (OAI-PMH client)
# - transformation logic
# - event sending to warehouse
# -----------------------------
class TestRunHarvester(unittest.TestCase):

    # Case 1: successful full harvest flow
    # Full successful harvest flow:
    # - record is fetched from OAI
    # - transformation is applied
    # - event is successfully sent to warehouse
    # - no deletion flag is present
    @patch("harvester.harvester_oaipmh.send_harvest_event")
    @patch("harvester.harvester_oaipmh.transformation_and_additional_metadata")
    @patch("harvester.harvester_oaipmh.Scythe")
    def test_run_harvester_success(self, mock_scythe, mock_transform, mock_send):

        # simulate successful metadata transformation step
        mock_transform.return_value = ("<datacite/>", None)

        # simulate succesful event delivery to warehouse
        mock_send.return_value = True

        # mock a single harvested OAI record
        mock_record = Mock()
        mock_record.header.identifier = "oai:test:123"
        mock_record.header.datestamp = "2025-01-01"
        mock_record.header.status = None

        xml_str = "<record><header><title>Test</title></header></record>"
        mock_record.xml = ET.fromstring(xml_str)

        # mock Scythe client
        mock_client = Mock()

        # list_records() returns one record
        mock_client.list_records.return_value = [mock_record]
        mock_scythe.return_value.__enter__.return_value = mock_client

        # input configuration for harvester run
        run_info = {
            "id": 1,
            "from_date": None,
            "until_date": "2025-01-01T00:00:00.000000+0000",
            "endpoint_config": {
                "name": "TEST",
                "code": "TEST",
                "harvest_url": "http://example.com/oai",
                "harvest_params": {
                    "metadata_prefix": "oai_dc"
                }
            }
        }

        result = run_harvester_oaipmh(run_info) # execute harvest pipeline
        self.assertTrue(result) # overall success path
        mock_transform.assert_called_once() # ensure transformation layer was used
        mock_send.assert_called_once() # ensure event was sent to warehouse
        payload = mock_send.call_args[0][0] # inspect payload sent to warehouse
        self.assertEqual(payload["record_identifier"], "123")
        self.assertEqual(payload["repo_code"], "TEST")
        self.assertFalse(payload["is_deleted"])


    # Case 2: deleted records should skip transformation
    # Deleted record handling:
    # - record is marked as deleted in OAI header
    # - transformation must NOT run
    # - record is still sent as deletion event
    @patch("harvester.harvester_oaipmh.send_harvest_event")
    @patch("harvester.harvester_oaipmh.transformation_and_additional_metadata")
    @patch("harvester.harvester_oaipmh.Scythe")
    def test_deleted_record_skips_transformation(
        self,
        mock_scythe,
        mock_transform,
        mock_send,
    ):
        # simulate successful event sending
        mock_send.return_value = True

        # mock a deleted OAI record
        mock_record = Mock()
        mock_record.header.identifier = "oai:test:123"
        mock_record.header.datestamp = "2025-01-01"
        mock_record.header.status = "deleted"
        xml_str = "<record><header><title>Test</title></header></record>"
        mock_record.xml = ET.fromstring(xml_str)

        # mock Scythe client
        mock_client = Mock()

        # list_records() returns one record
        mock_client.list_records.return_value = [mock_record]
        mock_scythe.return_value.__enter__.return_value = mock_client

        # input configuration for harvester run
        run_info = {
            "id": 1,
            "from_date": None,
            "until_date": "2025-01-01T00:00:00.000000+0000",
            "endpoint_config": {
                "name": "TEST",
                "code": "TEST",
                "harvest_url": "http://example.com/oai",
                "harvest_params": {
                    "metadata_prefix": "oai_dc"
                }
            }
        }

        result = run_harvester_oaipmh(run_info) # execute harvest pipeline
        self.assertTrue(result) # overall success path
        mock_transform.assert_not_called() # deleted records must not be transformed
        payload = mock_send.call_args[0][0] # ensure deletion flag is preserved in outgoing payload
        self.assertTrue(payload["is_deleted"])