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
    """ Main function excecuted by spark-submit command"""

    # add project root folder to sys.path to avoid modules import error
    sys.path.append(os.getcwd())

    args = _parse_arguments()

    with open("src/config.json", "r") as config_file:
        config = json.load(config_file)

    spark = SparkSession.builder.appName(config.get("app_name")).getOrCreate()

    job_path = f"jobs.{args.job}"
    print("========== job_path:", job_path)

    job_module = importlib.import_module(job_path)
    job_module.run_job(spark, config)


if __name__ == "__main__":
    main()
