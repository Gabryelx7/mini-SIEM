import time
import random
import os
from datetime import datetime, timezone
from faker import Faker
from core_producer import get_kafka_producer, produce_message

fake = Faker()

def generate_auth_log():
    # Simulate a failed login attempt or successful login
    status = random.choices(['failed', 'success'], weights=[0.8, 0.2])[0]
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "ssh_auth",
        "source_ip": fake.ipv4(),
        "username": fake.user_name(),
        "status": status,
        "port": 22,
        "message": f"SSH login {status} for user {fake.user_name()}"
    }

def main():
    producer = get_kafka_producer(os.getenv("KAFKA_PORT"))
    topic = "auth_logs"
    
    print(f"Starting simulated log generation for topic: {topic}...")
    
    try:
        while True:
            log_event = generate_auth_log()
            produce_message(producer, topic, log_event)
            
            # Flush periodically to ensure delivery
            producer.flush(timeout=0.5)
            
            # Sleep to simulate realistic sporadic traffic
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()