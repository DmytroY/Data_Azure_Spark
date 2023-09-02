# from shared.udfs import get_coordinates, get_geohash4
from src.shared.udfs import _get_latitude, _get_longitude, _get_geohash4, _get_country, _get_city, _get_distance


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

    def test_get_latitude_notexist2(self):
        expected_data = 0
        received_data = _get_latitude("US", "Washington", "2121 P St N W")
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

    def test_get_longitude_notexist2(self):
        expected_data = 0
        received_data = _get_longitude("US", "Washington", "2121 P St N W")
        assert received_data == expected_data


class TestUdfGetCountry:
    def test_get_country(self):
        expected_data = 'US'
        assert expected_data == _get_country(34.4454386, -83.1197032)

    def test_get_country_null(self):
        expected_data = 'NONE'
        assert expected_data == _get_country(0, 0)

class TestUdfGetCity:
    def test_get_city(self):
        expected_data = 'Cologne'
        assert expected_data == _get_city(50.939736, 6.961262)

    def test_get_town(self):
        expected_data = 'Lavonia'
        assert expected_data == _get_city(34.4454386, -83.1197032)

    def test_get_city_null(self):
        expected_data = 'None'
        assert expected_data == _get_city(0, 0)


class TestUdfGetGeohash:
    def test_get_geohash(self):
        test_data = ["38.66893", "-76.87629"]
        expected_data = "dqc7"
        assert expected_data == _get_geohash4(test_data[0], test_data[1])


class TestUdfGetDistance:
    def test_get_distance(self):
        expected_data = 279
        assert expected_data == _get_distance(52.2296756, 21.0122287, 52.406374, 16.9251681)

    def test_get_distance_zero(self):
        expected_data = 0
        assert expected_data == _get_distance(52.2296756, 21.0122287, 52.2296756, 21.0122287)
        assert expected_data == _get_distance(0, 0, 0, 0)
