from confluent_kafka import Producer
import json

def get_kafka_producer(broker_url="localhost:9092"):
    conf = {
        'bootstrap.servers': broker_url,
        'client.id': 'siem_log_generator'
    }
    return Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_message(producer, topic, message):
    producer.produce(
        topic, 
        value=json.dumps(message).encode('utf-8'), 
        callback=delivery_report
    )
    producer.poll(0)