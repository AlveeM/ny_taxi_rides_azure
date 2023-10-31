import contextlib
from pyspark import SparkConf
from pyspark.sql import SparkSession


@contextlib.contextmanager
def get_spark_session(conf: SparkConf):
    """
    Function that is wrapped by context manager
    Args:
      - conf(SparkConf): It is the configuration for the Spark session
    """

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    try:
        yield spark
    finally:
        spark.stop()
