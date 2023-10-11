""" 
spark job
"""
from pyspark.sql.functions import when, broadcast, col
from src.shared.udfs import get_geohash4_udf
from src.shared.udfs import get_latitude_udf, get_longitude_udf


def _extract_hotels(spark, config):
    """ Load hotel csv data, returns dataframe"""
    return(spark.read.option("header", True).csv(f"{config.get('source_data_path')}/hotels"))

def _extract_weather(spark, config):
    """ Load weater parquet data, returns dataframe"""
    return(spark.read.option("header", True).parquet(f"{config.get('source_data_path')}/weather"))

def _update_coordinates(data_frame):
    """ Update hotels Latitudes/Longitudes if absent """
    return data_frame.withColumn('Latitude', when(data_frame.Latitude.cast("int").isNull(),
                    get_latitude_udf(data_frame.Country, data_frame.City, data_frame.Address)) \
                            .otherwise(data_frame.Latitude)) \
                .withColumn('Longitude', when(data_frame.Longitude.cast("int").isNull(),
                    get_longitude_udf(data_frame.Country, data_frame.City, data_frame.Address)) \
                            .otherwise(data_frame.Longitude))

def _geohash(data_frame, new_cname, lat_cname, lon_cname):
    """ update df with new column for geohash.
    new_cname - name for new column,
    lat_cname, lon_cname - names of column where latitude and logitude are"""
    return data_frame.withColumn(new_cname, get_geohash4_udf(col(lat_cname), col(lon_cname)))

def _load(config, data_frame, filename):
    data_frame.write.option("header",True).mode("overwrite") \
        .parquet(f"{config.get('output_data_path')}/{filename}")

def run_job(spark, config):
    """ extract hotels, update Latitudes/Longitudes if is absent, add geohash.
    extract weather, add geohash.
    Left join weather and hotels data by generated 4-characters geohash.
    Load result"""
    h_df = _update_coordinates(_extract_hotels(spark, config)) 
    h_df = _geohash(h_df, "h_geohash", "Latitude", "Longitude")

    w_df = _extract_weather(spark, config)
    w_df = _geohash(w_df,"w_geohash","lat","lng")

    result = w_df.join(broadcast(h_df), h_df.h_geohash == w_df.w_geohash, "left")
    _load(config, result, "result")
    print(f"===== job done, join result contains {result.count()} records")

# def run_job(spark, config):
#     """ job to test read-write azure"""
#     h_df = _extract_hotels(spark, config)
#     print(f"===== hotels readed, it contains {h_df.count()} records")
#     w_df = _extract_weather(spark, config)
#     print(f"===== weather readed, it contains {w_df.count()} records")
#     _load(config, h_df, "result")
#     print(f"===== job done, result wrote")
