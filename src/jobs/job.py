from pyspark.sql.functions import when, broadcast
from src.shared.udfs import get_geohash4_udf
from src.shared.udfs import get_latitude_udf, get_longitude_udf


def _extract_hotel(spark, config):
    """ Load data: hotels, returns dataframe """
    # return(spark.read.option("header", True).csv(f"{config.get('source_data_path')}/hotels"))
    return(spark.read.option("header", True).csv(f"{config.get('samples_data_path')}/hotel_sample"))

def _extract_weather(spark, config):
    """ Load data: weather, returns dataframe """
    # return(spark.read.option("header", True).parquet(f"{config.get('source_data_path')}/weather"))
    return(spark.read.option("header", True).csv(f"{config.get('samples_data_path')}/weather_sample"))

def _update_coordinates(df):
    """ Update hotels Latitudes/Longitudes if absent """
    return df.withColumn('Latitude', when(df.Latitude.cast("int").isNull(),
                                        get_latitude_udf(df.Country, df.City, df.Address)) \
                                         .otherwise(df.Latitude)) \
            .withColumn('Longitude', when(df.Longitude.cast("int").isNull(),
                                      get_longitude_udf(df.Country, df.City, df.Address)) \
                                        .otherwise(df.Longitude))

def _hash_hotel(df):
    return df.withColumn('h_geohash', get_geohash4_udf(df.Latitude, df.Longitude))

def _hash_weather(df):
    return df.withColumn('w_geohash', get_geohash4_udf(df.lat, df.lng))


def run_job(spark, config):
    """ Check hotels data on incorrect (null) values (Latitude & Longitude).
    For incorrect values map (Latitude & Longitude) from OpenCage Geocoding API in job on fly (Via REST API).
    Generate geohash by Latitude & Longitude using one of geohash libraries (like geohash-java) with 4-characters length in extra column.
    Left join weather and hotels data by generated 4-characters geohash (avoid data multiplication and make you job idempotent)"""

    # hotels:load, update Latitudes/Longitudes, add gephash
    h = _hash_hotel(_update_coordinates(_extract_hotel(spark, config)))

    # weather:load, add gephash
    w = _hash_weather(_extract_weather(spark, config))

     # join on geohash
    result = w.join(broadcast(h), h.h_geohash == w.w_geohash, "left")

    # ======= load sample data =======
    result.coalesce(1) \
        .write.option("header",True) \
            .mode("overwrite") \
                .csv(f"{config.get('output_data_path')}/result")

    # # # ===== load =======
    # # result.write.option("header",True) \
    # #     .mode("overwrite") \
    # #         .parquet(f"{config.get('output_data_path')}/result")

    print(f"===== job done, join result contains {result.count()} records")
