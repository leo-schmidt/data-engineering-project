import csv
import json
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride import Ride
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC


class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path: str) -> List[Ride]:
        records = []
        with open(resource_path, "r") as f:
            reader = csv.reader(f)
            _header = next(reader)  # skip the header row
            for row in reader:
                records.append(Ride(arr=row))
        return records

    def publish_rides(self, topic: str, messages: List[Ride]):
        for ride in messages:
            try:
                record = self.producer.send(
                    topic=topic,
                    key=ride.pu_location_id,
                    value=ride,
                )
                print(
                    f"Record {ride.pu_location_id} successfully produced at offset {record.get().offset}"
                )
            except KafkaTimeoutError as e:
                print(e.__str__())


if __name__ == "__main__":
    config = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "key_serializer": lambda key: str(key).encode(),
        "value_serializer": lambda x: json.dumps(
            x.__dict__,
            default=str,
        ).encode("utf-8"),
    }
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)
    producer.publish_rides(topic=KAFKA_TOPIC, messages=rides)
