import time
import random
from datetime import datetime, timezone
from faker import Faker
from core_producer import get_kafka_producer, produce_message

fake = Faker()

def generate_firewall_event():
    traffic_class = random.choices(
        ['blocked', 'allowed', 'suspicious'],
        weights=[0.6, 0.3, 0.1]
    )[0]
    threat_levels = ['low', 'medium', 'high', 'critical']
    actions = ['blocked', 'dropped', 'rejected', 'allowed']
    protocols = ['TCP', 'UDP', 'ICMP']

    if traffic_class in ['blocked', 'suspicious'] and random.random() < 0.7:
        # Known bad IP
        source_ip = f"45.{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
    elif traffic_class == 'allowed' and random.random() < 0.05:
        # False negative or misconfiguration (5% chance)
        source_ip = f"45.{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
    else:
        source_ip = fake.ipv4()
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "firewall_event",
        "traffic_class": traffic_class,
        "action": random.choice(actions),
        "source_ip": source_ip,
        "source_port": random.randint(1024, 65535),
        "destination_ip": fake.ipv4(),
        "destination_port": random.choice([22, 80, 443, 3306, 5432, 8080]),
        "protocol": random.choice(protocols),
        "threat_level": random.choice(threat_levels),
        "bytes_transferred": random.randint(100, 1000000),
        "rule_name": f"RULE_{random.randint(1000, 9999)}",
        "firewall_id": f"FW-{random.randint(1, 10)}"
    }

def main():
    producer = get_kafka_producer("localhost:9092")
    topic = "firewall_events"
    
    print(f"Starting simulated firewall event generation for topic: {topic}...")
    
    try:
        while True:
            log_event = generate_firewall_event()
            produce_message(producer, topic, log_event)
            
            producer.flush(timeout=0.5)
            
            time.sleep(random.uniform(0.3, 1.5))
            
    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
