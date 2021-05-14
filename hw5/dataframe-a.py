from pyspark.sql import SparkSession
import json
import sys

spark = SparkSession \
    .builder \
    .appName("HW5") \
    .getOrCreate()


from pyspark import SparkContext

country_file = sys.argv[1]


sc = spark.sparkContext

country_df = spark.read.json(country_file)

"""##### query-a"""

country_df.select(country_df['Name']).where(country_df['Continent']=="North America").show()

print("/n")

answer = country_df.select(country_df['Name']).where(country_df['Continent']=="North America").show()
