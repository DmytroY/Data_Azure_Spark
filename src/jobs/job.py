from pyspark.sql.functions import when, broadcast, col
from src.shared.udfs import get_geohash4_udf
from src.shared.udfs import get_latitude_udf, get_longitude_udf


def _extract_hotels(spark, config):
    """ Load hotel csv data, returns dataframe"""
    return(spark.read.option("header", True).csv(f"{config.get('source_data_path')}/hotels"))

def _extract_weather(spark, config):
    """ Load weater parquet data, returns dataframe"""
    return(spark.read.option("header", True).parquet(f"{config.get('source_data_path')}/weather"))

def _update_coordinates(df):
    """ Update hotels Latitudes/Longitudes if absent """
    return df.withColumn('Latitude', when(df.Latitude.cast("int").isNull(),
                                        get_latitude_udf(df.Country, df.City, df.Address)) \
                                         .otherwise(df.Latitude)) \
            .withColumn('Longitude', when(df.Longitude.cast("int").isNull(),
                                      get_longitude_udf(df.Country, df.City, df.Address)) \
                                        .otherwise(df.Longitude))

def _geohash(df, new_cname, lat_cname, lon_cname):
    """ update df with new column for geohash.
    new_cname - name for new column,
    lat_cname, lon_cname - names of column where latitude and logitude are"""
    return df.withColumn(new_cname, get_geohash4_udf(col(lat_cname), col(lon_cname)))

def _load(config, df, filename):
    df.write.option("header",True).mode("overwrite") \
        .parquet(f"{config.get('output_data_path')}/{filename}")

def run_job(spark, config):
    """ extract hotels, update Latitudes/Longitudes if is absent, add geohash.
    extract weather, add geohash.
    Left join weather and hotels data by generated 4-characters geohash.
    Load result"""
    h = _geohash(_update_coordinates(_extract_hotels(spark, config, "csv", "hotel")),"h_geohash","Latitude","Longitude")
    w = _geohash(_extract_weather(spark, config, "parquet", "weather"),"w_geohash","lat","lng")
    result = w.join(broadcast(h), h.h_geohash == w.w_geohash, "left")
    _load(config, result, "result")
    print(f"===== job done, join result contains {result.count()} records")
