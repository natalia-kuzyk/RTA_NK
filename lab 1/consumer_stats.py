from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = {}
msg_count = 0

for message in consumer:
    tx = message.value
    category = tx["category"]
    amount = tx["amount"]

    if category not in stats:
        stats[category] = {
            "count": 0,
            "sum": 0,
            "min": amount,
            "max": amount
        }

    stats[category]["count"] += 1
    stats[category]["sum"] += amount

    if amount < stats[category]["min"]:
        stats[category]["min"] = amount

    if amount > stats[category]["max"]:
        stats[category]["max"] = amount

    msg_count += 1

    if msg_count % 10 == 0:
        print("\nStatystyki:")

        for c in stats:
            print(
                c,
                stats[c]["count"],
                stats[c]["sum"],
                stats[c]["min"],
                stats[c]["max"]
            )
