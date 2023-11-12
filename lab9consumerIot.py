# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
import json
import multiprocessing
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.animation as animation

stop_event = multiprocessing.Event()

temp_lectures = [0]
hum_lectures = [0]
wind_lectures = ['']
# Create figure for plotting
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
xs = []
ys = []

def animate(i, xs, ys, value):

    # Read temperature (Celsius) from TMP102
    

    # Add x and y to lists
    xs.append(dt.datetime.now().strftime('%H:%M:%S.%f'))
    ys.append(value)

    # Limit x and y lists to 20 items
    xs = xs[-20:]
    ys = ys[-20:]

    # Draw x and y lists
    ax.clear()
    ax.plot(xs, ys)

    # Format plot
    plt.xticks(rotation=45, ha='right')
    plt.subplots_adjust(bottom=0.30)
    plt.title('TMP102 Temperature over Time')
    plt.ylabel('Temperature (deg C)')


def encode_message(temperature, humidity, wind_direction):
    # Asumiendo que la dirección del viento se da en un conjunto conocido y ordenado
    wind_directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    wind_dir_encoded = wind_directions.index(wind_direction)

    # Codifica los valores en bytes
    encoded = bytearray([temperature, humidity, wind_dir_encoded])
    return encoded

def decode_message(encoded_message):
    wind_directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    
    # Decodifica los bytes
    temperature, humidity, wind_dir_encoded = encoded_message
    wind_direction = wind_directions[wind_dir_encoded]

    return temperature, humidity, wind_direction

# Ejemplo de Uso
encoded = encode_message(25, 50, 'NE')  # Codificar
decoded = decode_message(encoded)       # Decodificar


def main():
    consumer = KafkaConsumer(bootstrap_servers='lab9.alumchat.xyz:9092',auto_offset_reset='latest', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(['19062'])
    
    while not stop_event.is_set():      
        for message in consumer:
            data = decode_message(message.value)
            print("Temperature : " + str(message.value['temperature']) + u"\N{DEGREE SIGN}" + ' C'
                  "\nHumidity : " + str(message.value['humidity']) + ' %' +
                  "\nWind direction : " + str(message.value['wind_direction']) +
                  "\nTime: " + message.value['time'] +
                  "\n-----------------------------------"
            )
            temp_lectures.append(message.value['temperature']) 
            hum_lectures.append(message.value['humidity'])
            wind_lectures.append(message.value['wind_direction']) 
            with open('temp_record.txt', 'w') as file:
                file.write('\n'.join(str(temp) for temp in temp_lectures))
                
            with open('hum_record.txt', 'w') as file:
                file.write('\n'.join(str(temp) for temp in hum_lectures))

            with open('wind_record.txt', 'w') as file:
                file.write('\n'.join(str(temp) for temp in wind_lectures))
            # temp_lectures.append(message.value['temperature'])
            print(temp_lectures)
            if stop_event.is_set():
                break
    consumer.close()

if __name__ == '__main__':
    main()
    