import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json, col, to_timestamp, explode, date_format
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json

spark = SparkSession.builder.master("local[1]").appName('twitter-stream').getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


f = open('symbol_mapping.json', 'r')
stocks_dict = json.load(f)


schema = StructType(
    [
        StructField('id', StringType(), True),
        StructField('text', StringType(), True),
        StructField('created_at', StringType(), True)
    ]
)

def find_stock(text):
  stock_list = []
  for key, value in stocks_dict.items():
    if key in text:
      stock_list.append(value)
  return stock_list

find_stock = udf(find_stock, ArrayType(StringType()))

kafka_stock_raw_stream_df = spark.readStream.format("kafka").\
option("kafka.bootstrap.servers","localhost:9092").\
option("subscribe","stock_tweets_test").\
option("startingOffsets","earliest").load()

kafka_stock_stream_df = kafka_stock_raw_stream_df.\
selectExpr("cast(key as string) key", "cast(value as string) value")


kafka_stock_stream_df = kafka_stock_stream_df.withColumn("value", from_json("value", schema)).select(col('value.*'))
kafka_stock_stream_df = kafka_stock_stream_df.withColumn("created_at", to_timestamp('created_at', 'EEE MMM dd HH:mm:ss Z yyyy'))

kafka_stock_stream_df = kafka_stock_stream_df.select('id', 'created_at', find_stock("text").alias('stock'))
kafka_stock_stream_df = kafka_stock_stream_df.withColumn('stock', explode(kafka_stock_stream_df.stock))

kafka_stock_stream_df = kafka_stock_stream_df.withColumn("year", date_format(kafka_stock_stream_df.created_at, "yyyy")).\
withColumn("month", date_format(kafka_stock_stream_df.created_at, "MMM")).\
withColumn("day", date_format(kafka_stock_stream_df.created_at, "dd")).\
withColumn("hour", date_format(kafka_stock_stream_df.created_at, "HH")).\
withColumn("minute", date_format(kafka_stock_stream_df.created_at, "mm")).\
drop('id', 'created_at')

# stock_mentions_df = kafka_stock_stream_df.groupBy('stock', 'year', 'month', 'day', 'hour', 'minute').\
# count().withColumnRenamed('count', 'mentions')

kafka_stock_stream_df.writeStream.outputMode("append").format("console").start().awaitTermination()