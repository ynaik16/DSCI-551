from pyspark.sql import SparkSession
import json
import sys

spark = SparkSession \
    .builder \
    .appName("HW5") \
    .getOrCreate()


from pyspark import SparkContext

language_file = sys.argv[1]		# language file


cl_df = spark.read.json(language_file)

#cl_df.show()

answerd_d = cl_df.select(cl_df['Language']).filter(cl_df['CountryCode']=='CAN').show()

print(cl_df.select(cl_df['Language']).filter(cl_df['CountryCode']=='CAN').show())

