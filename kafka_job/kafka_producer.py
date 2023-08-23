from kafka import KafkaProducer
import pandas as pd
import time
import json


df = pd.read_csv("D:/lambdaClickData/lambda/speed/event.csv", dtype=str)


records = df.values.tolist()


producer = KafkaProducer(bootstrap_servers='localhost:9094',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))



       
for i, row in df.iterrows():
    if( i %  100) == 0:
        time.sleep(20)
    print(i)
    print(row.to_dict())
    producer.send('eventtransaction', row.to_dict())




