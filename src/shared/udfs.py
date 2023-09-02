""" User defined functions """
import os
from math import radians, cos, sin, asin, sqrt
import geohash2
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
from opencage.geocoder import OpenCageGeocode


def _get_latitude(country, city, address='') -> str:
    """ get latitude by address 
    Powered by https://opencagedata.com/ API.
    Usage example: _get_latitude("US", "Lavonia", "890 Ross Pl")
    returns: '34.4454386'
    """
    geocoder = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
    adr = address + ', ' + city + ', ' + country
    results = geocoder.geocode(adr)
    if results:
        return(results[0]['geometry']['lat'])
    return(0)

def _get_longitude(country, city, address='') -> str:
    """ get longitude by address 
    Powered by https://opencagedata.com/ API.
    Usage example: _get_latitude("US", "Lavonia", "890 Ross Pl")
    returns: '-83.1197032'
    """
    geocoder = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
    adr = address + ', ' + city + ', ' + country
    results = geocoder.geocode(adr)
    if results:
        return(results[0]['geometry']['lng'])
    return(0)

def _get_country(lat, lon) -> str:
    """ get country by coordinates. Powered by https://opencagedata.com/ API.
    Usage: _get_country(latitude, longitude),
    for example _get_address(34.4454386, -83.1197032),
    returns country code: 'US'
    """
    geocoder = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
    results = geocoder.reverse_geocode(float(lat), float(lon))
    return(str(results[0]['components'].get('country_code')).upper())

def _get_city(lat, lon) -> str:
    """ get city by coordinates. Powered by https://opencagedata.com/ API.
    Usage: _get_city(latitude, longitude),
    for example _get_city(34.4454386, -83.1197032),
    returns city 'Lavonia'
    """
    geocoder = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
    results = geocoder.reverse_geocode(float(lat), float(lon))
    city = results[0]['components'].get('city')
    return(str(city) if city else str(results[0]['components'].get('town')))


def _get_geohash4(lat=0, lon=0) -> str:
    """ get 4 first simbols of geohash by coordinates. Powered by geohash2 PyPi libriary
    Usage: get_geohash4(<latitude>, <longitude>)
    for example: 
        get_geohash4(34.4454386,-83.1197032) returns:"dnhg"
    """
    result = geohash2.encode(float(lat), float(lon))
    return result[0:4]

def _get_distance(lat1, lon1, lat2, lon2) -> int:

    # The math module contains a function named
    # radians which converts from degrees to radians.
    lon1 = radians(float(lon1))
    lon2 = radians(float(lon2))
    lat1 = radians(float(lat1))
    lat2 = radians(float(lat2))
      
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
 
    c = 2 * asin(sqrt(a))
    
    # Radius of earth in kilometers. Use 3956 for miles
    r = 6371
      
    # calculate the result
    return(c * r)

# register udf
get_latitude_udf = udf(_get_latitude, StringType())
get_longitude_udf = udf(_get_longitude, StringType())
get_country_udf = udf(_get_country, StringType())
get_city_udf = udf(_get_city, StringType())
get_geohash4_udf = udf(_get_geohash4, StringType())
get_distance_udf = udf(_get_distance, IntegerType())
