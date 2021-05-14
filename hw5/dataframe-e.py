from pyspark.sql import SparkSession
import json
import sys

spark = SparkSession \
    .builder \
    .appName("HW5") \
    .getOrCreate()


from pyspark import SparkContext
import pyspark.sql.functions as fc


print("Enter in format <python script> <country file>")

country_file = sys.argv[1]



country_df = spark.read.json(country_file)

#country_df.select(country_df['Continent']).show()

answer_e = country_df.select(country_df['Continent'], country_df['LifeExpectancy']) \
            .groupBy(country_df['Continent']) \
            .agg(fc.avg(country_df['LifeExpectancy']).alias('avg_le'),fc.count('*').alias('count'))

final_ans = answer_e[answer_e['count'] >= 20].orderBy(fc.desc('count')).limit(1)

final_ans = final_ans.select(final_ans['Continent'],final_ans['avg_le']).show()

