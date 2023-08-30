from pyspark.sql.functions import when, column
# from shared.udfs import get_coordinates_udf, get_geohash4
from src.shared.udfs import get_coordinates_udf, get_geohash4_udf


def _extract(spark, config):
    """ load data from csv file """
    return (spark.read.option("header", True).csv(f"{config.get('source_data_path')}/hotels"))

def _load_parquete(config, df):
    """ Save data to parquet file """
    df.write.mode("overwrite").parquet(
        f"{config.get('output_data_path')}/hotels"
    )

def _load_one_csv(config, df):
    """ Save data to csv file """
    df.coalesce(1).write.mode("overwrite").csv(
        f"{config.get('output_data_path')}/hotels"
    )


def run_job(spark, config):
    """ Check hotels data on incorrect (null) values (Latitude & Longitude).
    For incorrect values map (Latitude & Longitude) from OpenCage Geocoding API in job on fly (Via REST API).
    Generate geohash by Latitude & Longitude using one of geohash libraries (like geohash-java) with 4-characters length in extra column.
    
    Left join weather and hotels data by generated 4-characters geohash (avoid data multiplication and make you job idempotent)"""

    # extract data
    df_h = spark.read.option("header", True).csv(f"{config.get('source_data_path')}/hotels")
    df_w = spark.read.option("header", True).parquet(f"{config.get('source_data_path')}/weather")


    # Update hotels if Latitudes/Longitudes are absent
    df_h1 = df.withColumn('Latitude', when(df_h.Latitude.cast("int").isNull(), get_coordinates_udf(df_h.Country, df_h.City, df_h.Address)[0]) \
            .otherwise(df_h.Latitude)) \
        .withColumn('Longitude', when(df_h.Longitude.cast("int").isNull(), get_coordinates_udf(df_h.Country, df_h.City, df_h.Address)[1]) \
            .otherwise(df_h.Longitude))
    
    # add column with geohash to hotels
    df_h_hash = df_h1.withColumn("geohash", get_geohash4_udf(df_h1.Latitude, df_h1.Longitude))

    # add column with geohash to weather
    df_w_hash = df_w.withColumn("geohash", get_geohash4_udf(df_w.lat, df_w.lng))

    #
