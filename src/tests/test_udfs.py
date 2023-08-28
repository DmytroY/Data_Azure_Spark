# from shared.udfs import get_coordinates, get_geohash4
from src.shared.udfs import _get_coordinates, _get_geohash4
import pandas as pd

class TestUdfGetCoordinates:
    def test_get_coordinates_exist(self):
        expected_data = [34.4454386, -83.1197032]
        received_data = _get_coordinates("US", "Lavonia", "890 Ross Pl")
        assert received_data == expected_data

    def test_get_coordinates_notexist(self):
        expected_data = [0, 0]
        received_data = _get_coordinates("sdfs", "sdfs", "a;ldmapnq")
        assert received_data == expected_data

class TestUdfGetGeohash:
    def test_get_geohash(self):
        test_data = ["38.66893", "-76.87629"]
        expected_data = "dqc7"
        assert expected_data == _get_geohash4(test_data[0], test_data[1])
