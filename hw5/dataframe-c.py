from pyspark.sql import SparkSession
import json
import sys

spark = SparkSession \
    .builder \
    .appName("HW5") \
    .getOrCreate()


from pyspark import SparkContext

country_file = sys.argv[1]		# country file

country_df = spark.read.json(country_file)


sc = spark.sparkContext

country_df.select(country_df['Continent']).distinct().show()

answer_c = country_df.select(country_df['Continent']).distinct().show()