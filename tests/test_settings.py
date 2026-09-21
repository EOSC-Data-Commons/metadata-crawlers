import unittest
from unittest.mock import patch

from pydantic import ValidationError

from harvester.settings import (
    DevSettings,
    StagingSettings,
    LocalSettings,
    ProductionSettings,
    current_settings,
    get_settings,
    set_settings,
)


class TestSettings(unittest.TestCase):

    def tearDown(self) -> None:
        set_settings(None)

    # Case 1: verifies get_settings() returns a DevSettings or StagingSettings 
    # instance depending on the ENVIRONMENT env var.
    def test_get_settings_selects_class_by_environment(self) -> None:
        cases = [
            ("dev", DevSettings),
            ("staging", StagingSettings),
        ]
        for env_value, expected_cls in cases:
            with self.subTest(env = env_value):
                with patch.dict("os.environ", {"ENVIRONMENT": env_value}, clear = True):
                    settings = get_settings()
                    self.assertIsInstance(settings, expected_cls)

    # Case 2: verifies DevSettings and StagingSettings fall back to 
    # their own hardcoded default WAREHOUSE_API_URL when nothing overrides it
    def test_default_warehouse_api_urls(self) -> None:
        cases = [
            (DevSettings, "http://localhost:8080"),
            (StagingSettings, "http://192.168.10.6:8080"),
        ]
        for cls, expected_url in cases:
            with self.subTest(cls = cls.__name__):
                with patch.dict("os.environ", {}, clear = True):
                    settings = cls(_env_file = None)  # bypass .env, isolate the default
                    self.assertEqual(settings.WAREHOUSE_API_URL, expected_url)

    # Case 3: verifies LocalSettings (which has no built-in default) 
    # picks up WAREHOUSE_API_URL from the environment
    @patch.dict(
        "os.environ",
        {"ENVIRONMENT": "local", "WAREHOUSE_API_URL": "http://localhost:9999"},
        clear = True,
    )
    def test_local_settings_reads_url_from_environment(self) -> None:
        settings = get_settings()

        self.assertIsInstance(settings, LocalSettings)
        self.assertEqual(settings.WAREHOUSE_API_URL, "http://localhost:9999")

    # Case 4: verifies ProductionSettings() raises a ValidationError 
    # naming WAREHOUSE_API_URL when that field isn't supplied
    @patch.dict(
        "os.environ", 
        {"ENVIRONMENT": "production"}, 
        clear = True
    )
    def test_production_requires_warehouse_api_url(self) -> None:
        with self.assertRaises(ValidationError):
            ProductionSettings(_env_file = None)

    # Case 5: verifies ProductionSettings uses the 
    # WAREHOUSE_API_URL value supplied via the environment
    @patch.dict(
        "os.environ",
        {"ENVIRONMENT": "production", "WAREHOUSE_API_URL": "https://warehouse.example.com"},
        clear = True,
    )
    def test_production_settings(self) -> None:
        settings = get_settings()

        self.assertIsInstance(settings, ProductionSettings)
        self.assertEqual(settings.WAREHOUSE_API_URL, "https://warehouse.example.com")

    # Case 6: verifies set_settings() installs a given settings 
    # object and makes it the one current_settings() returns
    def test_set_settings_installs_custom_settings(self) -> None:
        custom_settings = DevSettings(WAREHOUSE_API_URL = "http://test:1234")

        result = set_settings(custom_settings)

        self.assertIs(result, custom_settings)
        self.assertIs(current_settings(), custom_settings)

    # Case 7: verifies current_settings() calls get_settings() only
    # once and returns the same cached instance on repeated calls
    @patch("harvester.settings.get_settings")
    def test_current_settings_is_cached(self, mock_get_settings) -> None:
        test_settings = DevSettings()
        mock_get_settings.return_value = test_settings
        set_settings(None)

        first = current_settings()
        second = current_settings()

        self.assertIs(first, test_settings)
        self.assertIs(second, test_settings)
        mock_get_settings.assert_called_once()