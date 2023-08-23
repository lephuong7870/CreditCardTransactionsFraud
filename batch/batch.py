from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, col,udf,split,explode,desc,rank, to_date
from pyspark.sql.functions import avg, current_timestamp, format_number
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType
from pyspark.sql.functions import sum, year, month
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

HDFS_NAMENODE = 'hdfs://namenode:9000'
if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("lambda arch batch job") \
        .getOrCreate()
    


    df = spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/lambda-arch/data/fraudTrain.csv")\
                .select( 'trans_date_trans_time' , 'cc_num' , 'merchant'  , 'category' , 'amt' , 'first' , 'last' , 
                         'gender' ,'street' , 'city' , 'state' , 'zip' , 'lat' , 'long' , 
                         'city_pop' , 'job' , 'dob' , 'trans_num' , 'merch_lat' , 'merch_long' , 'is_fraud'   )\
                .withColumn("trans_date_trans_time",to_date(col('trans_date_trans_time')))\
                .withColumn("cc_num",col('cc_num').cast(StringType()))\
                .withColumn("merchant",col('merchant').cast(StringType()))\
                .withColumn("category",col('category').cast(StringType()))\
                .withColumn("amt",col('amt').cast(DoubleType()))\
                .withColumn("first",col('first').cast(StringType()))\
                .withColumn("last",col('last').cast(StringType()))\
                .withColumn("gender",col('gender').cast(StringType()))\
                .withColumn("street",col('street').cast(StringType()))\
                .withColumn("city",col('city').cast(StringType()))\
                .withColumn("state",col('state').cast(StringType()))\
                .withColumn("zip",col('zip').cast(StringType()))\
                .withColumn("lat",col('lat').cast(DoubleType()))\
                .withColumn("long",col('long').cast(DoubleType()))\
                .withColumn("city_pop",col('city_pop').cast(StringType()))\
                .withColumn("job",col('job').cast(StringType()))\
                .withColumn("dob",to_date(col("dob")))\
                .withColumn("trans_num",col('trans_num').cast(StringType()))\
                .withColumn("merch_lat", col('merch_lat').cast(DoubleType()))\
                .withColumn("merch_long", col('merch_long').cast(DoubleType()))\
                .withColumn("is_fraud", col('is_fraud').cast(DoubleType()))
    

    df.printSchema()

    ## Table customer

    df_customer = df.select('cc_num' , 'first' , 'last' , 'gender' ,
                        'street' , 'city' , 'state' , 'zip' ,
                        'lat' , 'long' , 'job' , 'dob')
    df_customer = df_customer.distinct()
   

    min_amt  = df.groupBy('cc_num').min('amt')
    max_amt = df.groupBy('cc_num').max('amt')
    mean_amt = df.groupBy('cc_num').mean('amt')
    
    m1  = min_amt.join(max_amt , on='cc_num' , how='inner')
    m2 =  m1.join(mean_amt ,  on='cc_num' , how='inner')
    
    df_customer = df_customer.join(m2, on='cc_num' , how='inner') 
    df_customer = df_customer.withColumnRenamed("max(amt)" , "max_amt")\
                .withColumnRenamed("min(amt)" , "min_amt")\
                .withColumnRenamed("avg(amt)" , "mean_amt")

    df_customer.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="customer")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()
    

    ## Creat View Batch 
    # View Age Customer

    df_customer = df_customer.withColumn("age" ,F.floor(F.datediff(F.current_timestamp(), F.col("dob"))/365.25))
    df_group_age_customer = df_customer.groupBy('age').count()
    
    df_group_age_customer.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="view_age_count_customer")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()
    

    ## View Gender Customer
    df_group_gender_customer = df_customer.groupBy('gender').count()
    df_group_gender_customer.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="view_gender_count_customer")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()


  
   
    ## Table Transaction 
    df_transaction = df.withColumn("age" ,F.floor(F.datediff(F.col("trans_date_trans_time"), F.col("dob"))/365.25))\
                   .withColumn("distance" , F.sqrt(  F.pow( ( F.col('lat') -  F.col('merch_lat')),2 ) + F.pow( (F.col('long') - F.col('merch_long')),2 ) ) )\
                   .withColumnRenamed("trans_date_trans_time" , 'trans_time' )\
                   .withColumn("age", col('age').cast(IntegerType()))\
                   .select( 'cc_num' , 'trans_time' , 'trans_num' , 'category' ,
                            'merchant' , 'amt' ,'merch_lat' , 'merch_long' , 
                            'distance' , 'age' , 'is_fraud' )
    

    ## View Top 10  Transaction latest
    n = 10
    w = Window().partitionBy("cc_num").orderBy(col("trans_time").desc())
    df_top10 =   df_transaction.withColumn("row_number", row_number().over(w))\
                        .where(col("row_number") <= n)\
                        .select('cc_num' , 'trans_num' , 'category' ,  'amt', 'merchant' , 'trans_time' )
    
    df_top10.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="view_top10_latest")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()


    # table non_fraud_transaction

    df_non_fraud = df_transaction.filter(col('is_fraud') == 0)
    df_non_fraud.printSchema()
    df_non_fraud.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="non_fraud_transaction")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()
    

    #
    df_yes_traud = df_transaction.filter( col('is_fraud') == 1) 
    df_yes_traud.printSchema()
    df_yes_traud.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="fraud_transaction")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()




    
    ## Create View 
    ## View all Transaction by Category
    
    df_category_all_transaction = df_transaction.groupBy('category').count()
    df_category_all_transaction.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="view_transaction_by_category")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()
    


    df_transaction  = df_transaction.withColumn("year" ,year(col('trans_time')) )
    df_transaction = df_transaction.withColumn("month" , month(col('trans_time')))

    ## View Fraud Transaction by Category, year
    
    df_fraud_year = df_transaction.filter( col('is_fraud') == 1).groupBy('category','year').count()
    df_fraud_year.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="view_fraud_transaction_by_category_year")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()
    
    ## View Fraud Transaction by Month

    df_fraud_month = df_transaction.filter( col('is_fraud') == 1).groupby('month').count()
    df_fraud_month.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="creditcard", table="view_fraud_transaction_by_month")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()
    





    
              



