from pyspark.sql import SparkSession
import json
import sys

spark = SparkSession \
    .builder \
    .appName("HW5") \
    .getOrCreate()


from pyspark import SparkContext

print("First enter country file, then enter city file\n")

country_file = sys.argv[1]			# country file
city_file = sys.argv[2]				# city file


sc = spark.sparkContext

country_df = spark.read.json(country_file)

city_df = spark.read.json(city_file)

country_df.join(city_df, country_df.Capital == city_df.ID).select(country_df.Name,city_df.Name).show()

query_b_df = country_df.join(city_df, country_df.Capital == city_df.ID).select(country_df.Name,city_df.Name).show()