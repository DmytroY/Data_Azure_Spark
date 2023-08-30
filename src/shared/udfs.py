""" User defined functions """
import os
import geohash2
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, ArrayType, StringType
from opencage.geocoder import OpenCageGeocode


def _get_coordinates(country, city, address) -> list:
    """ get coordinates by addres. Powered by https://opencagedata.com/ API.
    Usage: _get_coordinates("<Counry, city, address>"),
    for example: _get_coordinates("US,Lavonia,890 Ross Pl")
    returns List with coordinates [latitude, longitude], for example: [34.4454386, -83.1197032]
    """
    geocoder = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
    adr = address + ', ' + city + ', ' + country
    results = geocoder.geocode(adr)
    if results:
        result = list((results[0]['geometry']['lat'], results[0]['geometry']['lng']))
        return result
    print("!!! =========== No coordinates for: ", adr)
    return ([0,0])

def _get_address(lat, lon):
    """ get city and country by coordinates. Powered by https://opencagedata.com/ API.
    Usage: _get_address(latitude, longitude),
    for example _get_address(34.4454386, -83.1197032),
    returns List with city and country code: ['Lavonia', 'US']
    """
    geocoder = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
    results = geocoder.reverse_geocode(float(lat), float(lon))
    
    if results[0]['components'].get('city'):
        city = results[0]['components'].get('city')
    else:
        city = results[0]['components'].get('town')

    country = results[0]['components'].get('country_code').upper()
    
    return(list((city, country)))

    
def _get_geohash4(lat, lon) -> str:
    """ get 4 first simbols of geohash by coordinates. Powered by geohash2 PyPi libriary
    Usage: get_geohash4(<latitude>, <longitude>)
    for example: 
        get_geohash4(34.4454386,-83.1197032) returns:"dnhg"
    """
    result = geohash2.encode(float(lat), float(lon))
    return result[0:4]

get_coordinates_udf = udf(_get_coordinates, ArrayType(FloatType()))
# get_address_udf = udf(_get_address,)
get_geohash4_udf = udf(_get_geohash4, StringType())
