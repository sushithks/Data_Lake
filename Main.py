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


def process_song_data(spark, input_data):

    # get filepath to song data file
    in_data = input_data + "in_data/*/*/*/*"

    # read song data file
    df = spark.read.json(in_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    data_table = df.select("data_id","title","artist_id","year","duration").drop_duplicates()



def main():
    spark = create_spark_session()



if __name__ == "__main__":
    main()