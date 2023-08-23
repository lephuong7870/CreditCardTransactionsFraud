# CreditCardTransactionsFraud

# Tool using: Cassandra , Docker , Kafka, Pyspark, Flaks, Bokeh

Lambda Architecture implementation using Kafka for stream ingestion, Spark for batch and stream processing, HDFS and Cassandra for storage and querying, Flask + Brokeh for live visualization, and Docker for deployment.

# Batch Processing
![image](https://github.com/lephuong7870/CreditCardTransactionsFraud/assets/92160581/6210d66c-04f1-41a7-b1c6-1b048864d4f8)



# Run Batch
docker exec spark-master /spark/bin/spark-submit --packages "com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" /batch/batch.py


# Streaming Processing
![image](https://github.com/lephuong7870/CreditCardTransactionsFraud/assets/92160581/ce0295d8-85bc-431a-98e4-562027f66368)



# Run Stream 
## Create topic in Kafka
docker exec kafka kafka-topics --create --topic eventtransaction --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

## Run Kafka Producer + Consumer
python3 kafka_producer.py <br/>

docker exec spark-master /spark/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" stream/kafka_consumer.py

## Result 
## Visualization: Flask + Brokeh


![Untitled](https://github.com/lephuong7870/CreditCardTransactionsFraud/assets/92160581/9b68e6fb-2425-4a66-8523-678555ab33c9)


![image](https://github.com/lephuong7870/CreditCardTransactionsFraud/assets/92160581/78aa4d24-0dd0-48ed-b0c9-9d4ed4cae1fa)






