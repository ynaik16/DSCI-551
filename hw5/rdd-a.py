from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("HW5") \
    .getOrCreate()

from pyspark import SparkContext
import sys

sc = spark.sparkContext

country_file = sys.argv[1]

country_df = spark.read.json(country_file)

rdd_a = country_df.rdd.map(list)

print(rdd_a.map(lambda x: (x[11],x[3])).filter(lambda x: x[1] == "North America").map(lambda x: x[0]).collect())