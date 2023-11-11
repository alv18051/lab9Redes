import json
import random
import numpy as np
import time
from confluent_kafka import Producer

def generate_sensor_data():
    temperature = np.clip(np.random.normal(50, 10), 0, 100)
    relative_humidity = int(np.clip(np.random.normal(50, 15), 0, 100))
    wind_direction = random.choice(['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'])

    return {
        "temperatura": round(temperature, 2), "humedad": relative_humidity, "direccion_viento": wind_direction
    }

# def generate_and_save_json_data(num_records, filename):
#     data = [generate_sensor_data() for _ in range(num_records)]
    
#     with open(filename, 'w') as file:
#         json_data = '[' + ',\n'.join(json.dumps(record) for record in data) + ']'
#         file.write(json_data)

conf = { #configuración de kafka
    #'bootstrap.servers': '157.245.244.105:9092',
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
    try:
        while True:
            data = generate_sensor_data()
            producer.produce('18051', json.dumps(data), callback=delivery_report)
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Data transmission interrumpida.")
    finally:
        print("Flushing queue...")
        producer.flush()
        print("Bye.")

producer = Producer(conf)
produce_sensor_data(15)#cambiar si se desea más de 15 segundos o menos



