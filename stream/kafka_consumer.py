from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf

spark_session = SparkSession \
    .builder \
    .appName("Streamer") \
    .config("spark.cassandra.connection.host","cassandra:9042" ) \
    .config("spark.executor.instances","1")\
    .getOrCreate()


df = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "eventtransaction") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

  

dataframe = df.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")

dataframe = dataframe.select(
   
    get_json_object(dataframe.value, '$.cc_num').alias('cc_num'),
    get_json_object(dataframe.value, '$.merchant').alias('merchant'),
    get_json_object(dataframe.value, '$.category').alias('category'),
    get_json_object(dataframe.value, '$.amt').alias('amt'),
    get_json_object(dataframe.value, '$.lat').alias('lat'),
    get_json_object(dataframe.value, '$.long').alias('long'),
    get_json_object(dataframe.value, '$.trans_num').alias('trans_num'),
    get_json_object(dataframe.value, '$.merch_lat').alias('merch_lat'),
    get_json_object(dataframe.value, '$.merch_long').alias('merch_long')

   
)


df = dataframe.withColumn("cc_num",col('cc_num').cast(StringType()))\
                .withColumn("merchant",col('merchant').cast(StringType()))\
                .withColumn("category",col('category').cast(StringType()))\
                .withColumn("amt",col('amt').cast(DoubleType()))\
                .withColumn("lat",col('lat').cast(DoubleType()))\
                .withColumn("long",col('long').cast(DoubleType()))\
                .withColumn("trans_num",col('trans_num').cast(StringType()))\
                .withColumn("merch_lat", col('merch_lat').cast(DoubleType()))\
                .withColumn("merch_long", col('merch_long').cast(DoubleType()))\
                .withColumn("trans_time" , current_timestamp()) \
                .withColumn("distance" , F.sqrt(  F.pow( ( F.col('lat') -  F.col('merch_lat')),2 ) + F.pow( (F.col('long') - F.col('merch_long')),2 ) ) )\
                .select( 'cc_num' , 'trans_time' , 'trans_num' , 'category' , 
                         'merchant' , 'amt' ,'merch_lat' , 'merch_long' ,  'distance' )

df.printSchema()

query = df.writeStream \
    .trigger(processingTime="5 seconds")\
    .format("org.apache.spark.sql.cassandra")\
    .option("checkpointLocation", '/tmp/stream/checkpoint/1/') \
    .option("keyspace", "creditcard") \
    .option("table", "eventtransaction") \
    .outputMode("append")\
    .start()

query.awaitTermination()  
