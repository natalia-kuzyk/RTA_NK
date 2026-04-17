from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
stores = ["Warszawa", "Kraków", "Gdańsk", "Wrocław"]
categories = ["elektronika", "odzież", "żywność", "książki"]

def generate_transaction():
    return {
        "tx_id": f'TX{random.randint(1000,9999)}',
        "user_id": f'u{random.randint(1, 20):02d}',
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(stores),
        "category": random.choice(categories),
        "timestamp": datetime.now().isoformat()
    }
    # TWÓJ KOD
    # Zwróć słownik z polami: tx_id, user_id, amount, store, category, timestamp
    pass

# TWÓJ KOD
# Pętla: generuj transakcję, wyślij do tematu 'transactions', wypisz, sleep 1s

for i in range(1000):
    tx = generate_transaction()
    producer.send('transactions', value=tx)
    print(f"[{i+1}] {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")
    time.sleep(1)

producer.flush()
producer.close()
