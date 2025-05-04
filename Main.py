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


def process_song_data(spark, input_data,output_data):

    # get filepath to song data file
    in_data = input_data + "in_data/*/*/*/*"

    # read song data file
    df = spark.read.json(in_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    data_table = df.select("data_id","title","artist_id","year","duration").drop_duplicates()


    # write songs table to parquet files partitioned by year and artist
    data_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()


    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")

def main():
    spark = create_spark_session()



if __name__ == "__main__":
    main()