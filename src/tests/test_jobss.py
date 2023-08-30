import pandas as pd
from src.jobs.hotels import run_job

class TestHotelsJob:
    def test_hello(self, spark_session):
        expected_data = spark_session.createDataFrame(
            [(1, "Alan", 17), (160646, "Kitty", 34)],
            ["id", "name", "age"],
        ).toPandas()

        received_data = run_job(spark_session, config={"app_name": "M06_SPARKBASICS_PYTHON_AZURE",}).toPandas()
        pd.testing.assert_frame_equal(received_data, expected_data, check_dtype=False)
