from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_times = {}

for message in consumer:
    tx = message.value
    user = tx["user_id"]
    time = datetime.fromisoformat(tx["timestamp"])

    if user not in user_times:
        user_times[user] = []

    user_times[user].append(time)

    nowe = []
    for t in user_times[user]:
        if (time - t).seconds <= 60:
            nowe.append(t)

    user_times[user] = nowe

    if len(user_times[user]) > 3:
        print("ALERT:", user)
