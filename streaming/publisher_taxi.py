import time
import random
from google.cloud import pubsub_v1
import json
from datetime import datetime, timedelta, timezone

topic_path = "projects/purwadika/topics/jcdeol3_capstone3_dimasadihartomo"
publisher = pubsub_v1.PublisherClient()


def random_timestamp():
    now = datetime.now(timezone.utc)
    delta = timedelta(minutes=random.randint(1, 60))
    return (now - delta).isoformat()

def generate_taxi_record():
    return {
        "VendorID": random.randint(1, 2),
        "lpep_pickup_datetime": random_timestamp(),
        "lpep_dropoff_datetime": random_timestamp(),
        "store_and_fwd_flag": random.choice([True, False]),
        "RatecodeID": random.randint(1, 5),
        "PULocationID": random.randint(1, 265),
        "DOLocationID": random.randint(1, 265),
        "passenger_count": random.randint(1, 6),
        "trip_distance": round(random.uniform(0.5, 20.0), 2),
        "fare_amount": round(random.uniform(3.0, 50.0), 2),
        "extra": round(random.uniform(0.0, 5.0), 2),
        "mta_tax": round(random.uniform(0.5, 1.0), 2),
        "tip_amount": round(random.uniform(0.0, 10.0), 2),
        "tolls_amount": round(random.uniform(0.0, 5.0), 2),
        "ehail_fee": None,
        "improvement_surcharge": round(random.uniform(0.0, 1.0), 2),
        "total_amount": round(random.uniform(10.0, 100.0), 2),
        "payment_type": random.randint(1, 4),
        "trip_type": random.randint(1, 2),
        "congestion_surcharge": str(round(random.uniform(0.0, 2.5), 2)) 
    }

while True:
    record = generate_taxi_record()
    message = json.dumps(record)
    print("Publishing:", message)
    publisher.publish(topic_path, message.encode("utf-8"))
    time.sleep(10)