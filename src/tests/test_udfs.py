"""unit tests of udfs.py by pylint"""
from src.shared.udfs import _get_latitude, _get_longitude, _get_geohash4

class TestUdfGetLatitude:
    """ test _get_latitude """
    def test_get_latitude_exist(self):
        """ test case when all arguments exist and correct """
        expected_data = 34.4454386
        received_data = _get_latitude("US", "Lavonia", "890 Ross Pl")
        assert received_data == expected_data

    def test_get_latitude_nostreet(self):
        """ test case when for limited arguments """
        expected_data = 34.4353309
        received_data = _get_latitude("US", "Lavonia")
        assert received_data == expected_data

    def test_get_latitude_notexist(self):
        """ test case when for not existed location """
        expected_data = 0
        received_data = _get_latitude("sdfs", "sdfs", "a;ldmapnq")
        assert received_data == expected_data


class TestUdfGetLongitude:
    """ test _get_longitude """
    def test_get_longitude_exist(self):
        """ test case when all arguments exist and correct """
        expected_data = -83.1197032
        received_data = _get_longitude("US", "Lavonia", "890 Ross Pl")
        assert received_data == expected_data

    def test_get_longitude_nostreet(self):
        """ test case when for limited arguments """
        expected_data = -83.1071278
        received_data = _get_longitude("US", "Lavonia")
        assert received_data == expected_data

    def test_get_longitude_notexist(self):
        """ test case when for not existed location """
        expected_data = 0
        received_data = _get_longitude("sdfs", "sdfs", "a;ldmapnq")
        assert received_data == expected_data


class TestUdfGetGeohash:
    """ test _get_geohash4 """
    def test_get_geohash(self):
        test_data = ["38.66893", "-76.87629"]
        expected_data = "dqc7"
        assert expected_data == _get_geohash4(test_data[0], test_data[1])
