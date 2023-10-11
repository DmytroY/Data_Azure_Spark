import os, sys
import json
import argparse
import importlib
from pyspark.sql import SparkSession

def _parse_arguments():
    """ Parse arguments provided by spark-submit command"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    return parser.parse_args()

def main():
    """ Main function excecuted by 'spark-submit src/main.py --job job' command"""
    print(" ====== main.py ======")
    # add project root folder to sys.path to avoid modules import error
    sys.path.append(os.getcwd())



    with open("src/config.json", "r") as config_file:
        config = json.load(config_file)

    spark = SparkSession.builder.appName(config.get("app_name")).getOrCreate()

    # configure spark for acces to storage account in Azure
    # spark.conf.set(f"fs.azure.account.key.{config.get('storage_account_name')}.blob.core.windows.net", {os.getenv('AZ_STORAGE_ACCES_KEY')})
    # spark.conf.set("fs.azure.account.key.styakovd1westeurope.blob.core.windows.net", "XBoGyDoaDIBAIKHa8J/reFtepsZbO6ddcoybdwzziqdc66WX0d29IvYvmN81j0LPP/pNk9rn6YzE+AStBRyDIA==")

    # # it was code for local run with "spark-submit src/main.py --job job"
    # args = _parse_arguments()
    # job_path = f"jobs.{args.job}"
    # print(" ====== job_path = jobs.", args.job)
    # job_module = importlib.import_module(job_path)

    # run with "spark-submit src/main.py"
    job_module = importlib.import_module("jobs.job")
    job_module.run_job(spark, config)


if __name__ == "__main__":
    main()
