import time
import random
from datetime import datetime, timezone
from faker import Faker
from core_producer import get_kafka_producer, produce_message

fake = Faker()

def generate_api_gateway_log():
    status_code = random.choices(
        [200, 201, 400, 401, 403, 500, 502],
        weights=[60, 10, 10, 12, 5, 2, 1]
    )[0]
    
    endpoints = ['/login', '/logout', '/users', '/auth/token', '/api/data']
    methods = ['POST', 'GET', 'PUT', 'DELETE']
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "api_gateway_request",
        "method": random.choice(methods),
        "endpoint": random.choice(endpoints),
        "status": status_code,
        "client_ip": fake.ipv4(),
        "username": fake.user_name() if status_code in [200, 201] else "unknown",
        "response_time_ms": random.randint(50, 5000),
        "user_agent": fake.user_agent(),
        "request_id": fake.uuid4()
    }

def main():
    producer = get_kafka_producer("localhost:9092")
    topic = "api_gateway_logs"
    
    print(f"Starting simulated API gateway log generation for topic: {topic}...")
    
    try:
        while True:
            log_event = generate_api_gateway_log()
            produce_message(producer, topic, log_event)
            
            producer.flush(timeout=0.5)
            
            time.sleep(random.uniform(0.2, 1.0))
            
    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
