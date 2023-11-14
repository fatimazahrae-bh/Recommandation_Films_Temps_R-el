# import requests 
# from confluent_kafka import Producer
# import time
# import json
# from kafka import KafkaProducer 
# from kafka.errors import NoBrokersAvailable
# import logging
# import sys


# # Configuration du producteur Kafka
# server = 'localhost:9092'
# topic_name = 'movie_ratings'

# # Création d'une instance de producteur
# try:
#     producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# except NoBrokersAvailable as ne:
#     logging.error('No brokers available')
#     sys.exit()

# movie_id = 2
# while True:
#     api_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key=6cfa125625b141bdafe97606330ede6f"
#     headers = {
#         "accept": "application/json"
#     }
#     response = requests.get(api_url, headers=headers)
#     movie_id += 1
#     if response.status_code == 200:
#         data = json.dumps(response.json())
#         producer.send(topic_name, key="movie".encode('utf-8'), value=data)
#         time.sleep(10)
#         producer.flush()
#         print(response.json())
#     else:
#         print(response.status_code)


import requests 
from confluent_kafka import Producer
import time
import json

topic_name = "movie_ratings"

kafka_config = {
    "bootstrap.servers": "localhost:9092", 
}

producer = Producer(kafka_config)

movie_id = 2
while True:
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key=166d5fe76b80ebd8dfa5158f8dc108a7"
    headers = {
        "accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    movie_id += 1
    if response.status_code == 200:
        data = json.dumps(response.json())
        producer.produce(topic_name, key="movie", value=data)
        time.sleep(10)
        producer.flush()
        print(response.json())
    else:
        print(response.status_code)
    
   


