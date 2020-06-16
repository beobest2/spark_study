import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test01').getOrCreate()

events_df = spark.read.json("../../videodata.json")

print("events_df.head(10) : ", events_df.head(10))
print("count() : ", events_df.count())

open_cnt = events_df.where(events_df.state == "open").count()

events_df.select(events_df.show_id) \
    .where(events_df.state == "open") \
    .groupBy(events_df.show_id).count().show()
