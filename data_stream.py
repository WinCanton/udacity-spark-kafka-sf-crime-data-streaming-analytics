import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
            StructField("address", StringType(), True),
            StructField("address_type", StringType(), True),
            StructField("agency_id", StringType(), True),
            StructField("call_date", StringType(), True),
            StructField("call_date_time", StringType(), True),
            StructField("call_time", StringType(), True),
            StructField("city", StringType(), True),
            StructField("common_location", StringType(), True),
            StructField("crime_id", StringType(), True),
            StructField("disposition", StringType(), True),
            StructField("offense_date", StringType(), True),
            StructField("original_crime_type_name", StringType(), True),
            StructField("report_date", StringType(), True),
            StructField("state", StringType(), True),    
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "udacity.project.sfcrime.analytics") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(["original_crime_type_name", "disposition"])

    # count the number of original crime type
    agg_df = distinct_table.groupBy(distinct_table.original_crime_type_name).count().orderBy('count',ascending=False)

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .format("console") \
        .outputMode("complete") \
        .queryName('query_agg_df') \
        .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.option("multiLine", True).json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .queryName('join_query') \
        .start()


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.default.parallelism", 2) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
