def run_job(spark, config):
    """ Run hotels job """
    print(f"================ hotels.py: app name is {config.get('app_name')}")
    df = spark.createDataFrame(
            [(1, "Alan", 17), (160646, "Kitty", 34)],
            ["id", "name", "age"],
        )
    return df
