import json
import random
import numpy as np

def generate_sensor_data():
    temperature = np.clip(np.random.normal(50, 10), 0, 100)
    relative_humidity = int(np.clip(np.random.normal(50, 15), 0, 100))
    wind_direction = random.choice(['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'])

    return {
        "temperatura": round(temperature, 2), "humedad": relative_humidity, "direccion_viento": wind_direction
    }

def generate_and_save_json_data(num_records, filename):
    data = [generate_sensor_data() for _ in range(num_records)]
    
    with open(filename, 'w') as file:
        json_data = '[' + ',\n'.join(json.dumps(record) for record in data) + ']'
        file.write(json_data)



generate_and_save_json_data(50, 'sensor_data.json')


