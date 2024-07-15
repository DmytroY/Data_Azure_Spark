""" functional test for job.py by pytest """
import os
import shutil
import src.jobs.job as job

class TestJob:
    """ test job.py """
    def test_run_job(self, spark_session, mocker):
        """ mock extracting data from file then run job with this test data
        successfull job shoul create file with resulting data - check it"""
        test_config = {"output_data_path": "test_data_output"}

        if os.path.exists(test_config.get("output_data_path")):
            shutil.rmtree(test_config.get("output_data_path"))

        test_data_h = spark_session.createDataFrame(
            [("2508260900870","UNA Maison Milano","IT", "Milan", "Via Mazzini 4",	"45.4633289", "9.1884242"),
             ("970662608900", "Palomar Washington Dc, A Kimpton Hotel", "US", "Washington",	"2121 P St N W"	, "", "")],
            ["Id",	"Name",	"Country",	"City",	"Address",	"Latitude",	"Longitude"]
        )
        mocker.patch.object(job, "_extract_hotels")
        job._extract_hotels.return_value = test_data_h

        test_data_w = spark_session.createDataFrame(
            [(8.18181,	45.0767,	74.9,	23.8,	"2017-08-15",	"2017",	"8",	"15"),
            (8.38635,	45.0767,	74.5,	23.6,	"2017-08-15",	"2017",	"8",	"15")],
            ["lng",	"lat",	"avg_tmpr_f",	"avg_tmpr_c",	"wthr_date",	"year",	"month",	"day"]
        )
        mocker.patch.object(job, "_extract_weather")
        job._extract_weather.return_value = test_data_w

        job.run_job(spark_session, test_config)
        assert os.path.exists(test_config.get("output_data_path"))
