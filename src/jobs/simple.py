"""
simpliest job for experiment with spark-submit
"""

def run_job(spark, config):
    print("======= simple.py")
    df = spark.read.option("header", True).csv(f"{config.get('source_data_path')}/hotels")
    print("======= df readed")
    result = df.filter(df.Country == "US")
    print("======= df filtered")
    result.write.option("header",True).mode("overwrite").csv(f"{config.get('output_data_path')}/testout")
    print("======= simple job done")