from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load dataset
df = pd.read_csv("../supply_chain.csv")
df = df.dropna()

# df = df.sample(1000, random_state=42)

print("Starting Kafka producer...")

for i, (_, row) in enumerate(df.iterrows()):
    message = row.to_dict()
    producer.send('logistics_stream', message)
    print(f"Sent row {i+1}: {message}")
    time.sleep(1) 
