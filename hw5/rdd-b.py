from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("HW5") \
    .getOrCreate()

from pyspark import SparkContext
import sys

sc = spark.sparkContext

print("Enter in format <python script> <city file> <country file>")

country_file = sys.argv[1]
city_file = sys.argv[2]

country_df = spark.read.json(country_file)

city_df = spark.read.json(city_file)


rdd_country = country_df.rdd.map(list)
rdd_city = city_df.rdd.map(list)

#rdd_country.take(1)

a = rdd_country.map(lambda x: (x[11],x[0]))
#a.collect()

#type(a)

#rdd_city.first()

b = rdd_city.map(lambda x: (x[3],x[2]))
#b.collect()

#type(b)

new_rdd = a.join(b)
rdd_query_b_answer = new_rdd.map(lambda x: (x[0],x[1][0])).collect()

print(rdd_query_b_answer)