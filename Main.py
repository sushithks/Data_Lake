from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark