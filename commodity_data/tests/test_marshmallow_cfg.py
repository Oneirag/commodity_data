"""
Tests marshmallow configurations, to check they are syntactically correct
"""
import unittest
import marshmallow_dataclass
import marshmallow
from commodity_data.series_config import BarchartConfig, OmipConfig
from commodity_data.downloaders.barchart.barchart_downloader import barchart_cfg
from commodity_data.downloaders.default_config import omip_cfg


class TestMarshmallow(unittest.TestCase):

    def test_omip_main_config(self):
        """Tests the Omip config, no exception should be risen"""
        omip = marshmallow_dataclass.class_schema(OmipConfig)()
        for item in omip_cfg:
            print(omip.load(item))

    def test_barchart_main_config(self):
        """Tests the barchart config, no exception should be risen"""
        barchart = marshmallow_dataclass.class_schema(BarchartConfig)()
        for item in barchart_cfg:
            print(barchart.load(item))

    def test_barchart_sample_config(self):
        """Tests a Barchart config and parses with marshmallow_dataclass to make sure it works properly"""
        cfg = [
            {
                "download_cfg": {
                    "symbol": "EJEMPLO",
                    "product": "Y1",
                },
                "commodity_cfg": {
                    "commodity": "CO2",
                    "instrument": "EUA",
                    "area": "EU",
                },
                "except": True,  # Additional field for testing
            },
            {
                "download_cfg": {
                    "symbol": "EJEMPLO2",
                    "product": "Y",
                    "expiry": "2021-12-01",
                },
                "commodity_cfg": {
                    "commodity": "CO2",
                    "instrument": "EUA",
                    "area": "EU",
                }
            },
            {
                "download_cfg": {
                    "symbol": "EJEMPLO2",
                    "product": "Y",
                    "expiry": "hola que tal",
                },
                "commodity_cfg": {
                    "commodity": "CO2",
                    "instrument": "EUA",
                    "area": "EU",
                },
                "except": True,  # Additional field for testing
            },
            {
                "download_cfg": {
                    "symbol": "EJEMPLO3",
                },
                "commodity_cfg": {
                    "commodity": "CO2",
                    "instrument": "EUA",
                    "area": "EU",
                }
            },
            {
                "download_cfg": {
                    "symbol": "EJEMPLO5",
                },
                "commodity_cfg": dict(commodity='CO2', instrument='EUA', area='EU')
            },
        ]

        barchart = marshmallow_dataclass.class_schema(BarchartConfig)()
        for item in cfg:
            must_raise_exception = item.pop("except", False)
            print(item)
            if must_raise_exception:
                with self.assertRaises(marshmallow.exceptions.ValidationError, msg="Exception not risen!!!") as cm:
                    barchart.load(item)
                print(f"ValidationError exception risen, as expected: {cm.exception}")
            else:
                example = barchart.load(item)
                print(example)
                print(example.commodity_cfg)


if __name__ == '__main__':
    unittest.main()
