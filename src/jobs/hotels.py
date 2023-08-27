from pyspark.sql.functions import when, column
# from src.shared.udfs import coordinates, geohash
from shared.udfs import get_coordinates, get_geohash4


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

    # read hotels to dataframe
    df = _extract(spark, config)

    # Update where Latitudes/Longitudes are absent
    print("======== updating Latitude/Longitude")
    df_upd = df.withColumn('Latitude', when(df.Latitude.cast("int").isNull(), get_coordinates(df.Country, df.City, df.Address)[0]) \
            .otherwise(df.Latitude)) \
        .withColumn('Longitude', when(df.Longitude.cast("int").isNull(), get_coordinates(df.Country, df.City, df.Address)[1]) \
            .otherwise(df.Longitude))
    
    # add column with geohash
    print("======== adding GEOHASH")
    df_hash = df_upd.withColumn("geohash", get_geohash4(df_upd.Latitude, df_upd.Longitude))

    print(df_hash.show(20))

    print("============== Job is done ==========")


