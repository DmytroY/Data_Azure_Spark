from pyspark.sql.functions import when, col, broadcast
from src.shared.udfs import get_latitude_udf, get_longitude_udf, get_country_udf, get_city_udf, get_distance_udf, get_geohash4_udf


def run_job(spark, config):
    """ Check hotels data on incorrect (null) values (Latitude & Longitude).
    For incorrect values map (Latitude & Longitude) from OpenCage Geocoding API in job on fly (Via REST API).
    Generate geohash by Latitude & Longitude using one of geohash libraries (like geohash-java) with 4-characters length in extra column.
    Left join weather and hotels data by generated 4-characters geohash (avoid data multiplication and make you job idempotent)"""

    # spark.udf.register("get_coordinates_udf", get_coordinates_udf)

    # ======  extract data ==========
    # df_h = spark.read.option("header", True).csv(f"{config.get('source_data_path')}/hotels")
    # df_w = spark.read.option("header", True).parquet(f"{config.get('source_data_path')}/weather")

    h = spark.read.option("header", True).csv(f"{config.get('samples_data_path')}/hotel_sample")
    w = spark.read.option("header", True).csv(f"{config.get('samples_data_path')}/weather_sample")


    # ======  fix incorrect data ===========
    # Update hotels if Latitudes/Longitudes are absent
    h = h.withColumn('Latitude', when(h.Latitude.cast("int").isNull(),
                                      get_latitude_udf(h.Country, h.City, h.Address)) \
                                        .otherwise(h.Latitude)) \
        .withColumn('Longitude', when(h.Longitude.cast("int").isNull(),
                                      get_longitude_udf(h.Country, h.City, h.Address)) \
                                        .otherwise(h.Longitude))
    
    print("======== check that coordinates are fixed ===========")
    h.filter(h.Name == 'Palomar Washington Dc, A Kimpton Hotel').show()

    # save to check in csv
    h.coalesce(1) \
        .write.option("header",True) \
            .mode("overwrite") \
                .csv(f"{config.get('output_data_path')}/hotel_sample_allcoord")
    
    # find and fix if hotel City or Country does not correspond to coordinates
    # from hotel data create pivot table with Country, City
    # and add calculated by Opencage API Latitude and longitude of city
    p = h.select(col("Country").alias("country"), col("City").alias("city"), ).distinct()
    p = p.withColumn('lat', get_latitude_udf(p.country, p.city)) \
        .withColumn('lon', get_longitude_udf(p.country, p.city))
    
    print("======== check p dataframe ===========")
    p.show()
    
#============
    # join h (hotels) and p (correct coordinates of city centres)
    h = h.join(broadcast(p), (h.Country == p.country) & (h.City == p.city), "left") \
        .select(h.Id, h.Name, h.Country, h.City, h.Address, h.Latitude, h.Longitude, p.lat, p.lon)

    # if distance between hotel coordinates and city center > 50 km
    # then city and country in hotel adress in wrong
    # update data with correct country and city
    h = h.withColumn("Country", when( get_distance_udf(h.Latitude, h.Longitude, h.lat, h.lon) > 50,
                                     get_country_udf(h.Latitude, h.Longitude)). \
                                        otherwise(h.Country)) \
        .withColumn("City", when( get_distance_udf(h.Latitude, h.Longitude, h.lat, h.lon) > 50,
                                 get_city_udf(h.Latitude, h.Longitude)) \
                                    .otherwise(h.City))
    droplist =["lat", "lon"]
    h = h.select([col for col in h.columns if col not in droplist])
    h.show()
#===============

    # ======= add geohash =======
    h = h.withColumn('h_geohash', get_geohash4_udf(h.Latitude, h.Longitude))
    h.coalesce(1) \
        .write.option("header",True) \
            .mode("overwrite") \
                .csv(f"{config.get('output_data_path')}/hotel_sample_hash")

    w = w.withColumn('w_geohash', get_geohash4_udf(w.lat, w.lng))
    w.coalesce(1) \
        .write.option("header",True) \
            .mode("overwrite") \
                .csv(f"{config.get('output_data_path')}/weather_sample_hash")
    
    print(f"===== hotels has {h.count()} records")
    print(f"===== weather has {w.count()} records")
    # print("====== join")

    # ======= join on geohash =========
    result = w.join(broadcast(h), h.h_geohash == w.w_geohash, "left")

    # ======= load =======
    result.coalesce(1) \
        .write.option("header",True) \
            .mode("overwrite") \
                .csv(f"{config.get('output_data_path')}/result")
    h.show()
    w.show()
    result.show()

