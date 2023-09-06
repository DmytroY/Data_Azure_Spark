""" User defined functions """
import os
import time
import geohash2
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
from opencage.geocoder import OpenCageGeocode


def _get_latitude(country, city, address='') -> str:
    """ get latitude by address 
    Powered by https://opencagedata.com/ API.
    Usage example: _get_latitude("US", "Lavonia", "890 Ross Pl")
    returns: '34.4454386'  """
    # we use free opencage account, only 1 API request/sec is alloved 
    time.sleep(1)

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
    returns: '-83.1197032'    """  

    # we use free opencage account, only 1 API request/sec is alloved 
    time.sleep(1)

    geocoder = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
    adr = address + ', ' + city + ', ' + country
    results = geocoder.geocode(adr)
    if results:
        return(results[0]['geometry']['lng'])
    return(0)

def _get_geohash4(lat=0, lon=0) -> str:
    """ get 4 first simbols of geohash by coordinates. Powered by geohash2 PyPi libriary
    Usage: _get_geohash4(<latitude>, <longitude>)
    for example: get_geohash4(34.4454386,-83.1197032) returns:"dnhg"   """
    result = geohash2.encode(float(lat), float(lon))
    return result[0:4]

# register udf
get_latitude_udf = udf(_get_latitude, StringType()).asNondeterministic()
get_longitude_udf = udf(_get_longitude, StringType()).asNondeterministic()
get_geohash4_udf = udf(_get_geohash4, StringType())
