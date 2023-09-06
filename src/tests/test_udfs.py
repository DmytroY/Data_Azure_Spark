# from shared.udfs import get_coordinates, get_geohash4
from src.shared.udfs import _get_latitude, _get_longitude, _get_geohash4

class TestUdfGetLatitude:
    def test_get_latitude_exist(self):
        expected_data = 34.4454386
        received_data = _get_latitude("US", "Lavonia", "890 Ross Pl")
        assert received_data == expected_data

    def test_get_latitude_nostreet(self):
        expected_data = 34.4353309
        received_data = _get_latitude("US", "Lavonia")
        assert received_data == expected_data

    def test_get_latitude_notexist(self):
        expected_data = 0
        received_data = _get_latitude("sdfs", "sdfs", "a;ldmapnq")
        assert received_data == expected_data


class TestUdfGetLongitude:
    def test_get_longitude_exist(self):
        expected_data = -83.1197032
        received_data = _get_longitude("US", "Lavonia", "890 Ross Pl")
        assert received_data == expected_data

    def test_get_longitude_nostreet(self):
        expected_data = -83.1071278
        received_data = _get_longitude("US", "Lavonia")
        assert received_data == expected_data

    def test_get_longitude_notexist(self):
        expected_data = 0
        received_data = _get_longitude("sdfs", "sdfs", "a;ldmapnq")
        assert received_data == expected_data


class TestUdfGetGeohash:
    def test_get_geohash(self):
        test_data = ["38.66893", "-76.87629"]
        expected_data = "dqc7"
        assert expected_data == _get_geohash4(test_data[0], test_data[1])
