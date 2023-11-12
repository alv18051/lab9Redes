import json
import random
import numpy as np
import time
from confluent_kafka import Producer

def generate_sensor_data():
    # Genera solo la temperatura como un entero
    temperature = int(np.clip(np.random.normal(50, 10), 0, 100))
    return temperature

conf = {
    'bootstrap.servers': 'lab9.alumchat.xyz:9092',
    'client.id': 'sensor-data-producer',
    'linger.ms': 1,
    'acks': 1,
    'retries': 5,
    'retry.backoff.ms': 1000
}

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_sensor_data(interval):
    producer = Producer(conf)
    try:
        while True:
            temperature = generate_sensor_data()
            # Asegúrate de que el mensaje es de 3 bytes
            message = str(temperature).zfill(3)[:3].encode()
            producer.produce('18051', message, callback=delivery_report)
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Data transmission interrumpida.")
    finally:
        print("Flushing queue...")
        producer.flush()
        print("Bye.")

produce_sensor_data(15)  # Cambiar si se desea más de 15 segundos o menos
