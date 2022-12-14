from confluent_kafka import Producer
from flask import jsonify
import json
import time
import requests
from api_keys import username, password


####################

def get_payload(data, ASIN):
    payload = {}
    payload['ASIN'] = ASIN
    try:
        payload['name'] = str(data['name'])
    except:
        pass

    try:
        payload['product_information'] = str(data['product_information'])
    except:
        pass

    try:
        payload['brand'] = str(data['brand'])
    except:
        pass

    try:
        payload['brand_url'] = str(data['brand_url'])
    except:
        pass

    try:
        payload['full_description'] = str(data['full_description'])
    except:
        pass

    try:
        payload['pricing'] = str(data['pricing'])
    except:
        pass

    try:
        payload['list_price'] = str(data['list_price'])
    except:
        pass

    try:
        payload['availability_status'] = str(data['availability_status'])
    except:
        pass

    try:
        payload['images'] = str(data['images'])
    except:
        pass

    try:
        payload['product_category'] = str(data['product_category'])
    except:
        pass

    try:
        payload['average_rating'] = str(data['average_rating'])
    except:
        pass

    try:
        payload['small_description'] = str(data['small_description'])
    except:
        pass

    try:
        payload['feature_bullets'] = str(data['feature_bullets'])
    except:
        pass

    try:
        payload['total_reviews'] = str(data['total_reviews'])
    except:
        pass

    try:
        payload['total_answered_questions'] = str(
            data['total_answered_questions'])
    except:
        pass

    try:
        payload['model'] = str(data['model'])
    except:
        pass

    try:
        payload['customization_options'] = str(data['customization_options'])
    except:
        pass

    try:
        payload['seller_id'] = str(data['seller_id'])
    except:
        pass

    try:
        payload['seller_name'] = str(data['seller_name'])
    except:
        pass

    try:
        payload['fulfilled_by_amazon'] = str(data['fulfilled_by_amazon'])
    except:
        pass

    try:
        payload['fast_track_message'] = str(data['fast_track_message'])
    except:
        pass

    try:
        payload['aplus_present'] = str(data['aplus_present'])
    except:
        pass

    return payload


def create_sink_connector():
    data = {
        "name": "mysql-kafkaproject-sink",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": f"jdbc:mysql://localhost:3306/Kafka?user={username}&password={password}",
            "topics": "kafkaproject",
            "insert.mode.databaselevel": "true",
            "table.name.format": "amazon_products",
            "auto.offset.reset": "earliest",
            "auto.create": "true",
            "auto.evolve": "true",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true"

        }
    }

    # create sink connector
    url = 'http://127.0.0.1:8083/connectors'
    response = requests.post(url, json=data)
    print(response.json())


def delete_sink_connector():
    # delete conectror
    requests.delete("http://127.0.0.1:8083/connectors/mysql-kafkaproject-sink")


def to_consumer(data, ASIN):
    data = data.json()

    data.update({"ASIN": ASIN})

    # for consumer
    producer.poll(1)
    producer.produce('kafkaconsumer', json.dumps(data).encode('utf-8'))
    producer.flush()


def mainproducer():
    for i in range(249):
        with open('random-items.txt', 'r') as file:
            ASIN = file.read()

        ASIN = ASIN.split('\n')
        data = requests.get(f'http://localhost:5000/products/{i}')

        to_consumer(data, ASIN[i])

        payload = get_payload(data.json(), ASIN[i])

        data = json.dumps({
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "string",
                        "optional": True,
                        "field": "ASIN"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "name"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "product_information"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "brand"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "brand_url"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "full_description"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "pricing"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "list_price"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "availability_status"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "images"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "product_category"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "average_rating"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "small_description"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "feature_bullets"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "total_reviews"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "total_answered_questions"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "model"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "customization_options"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "seller_id"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "seller_name"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "fulfilled_by_amazon"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "fast_track_message"
                    },
                    {
                        "type": "string",
                        "optional": True,
                        "field": "aplus_present"
                    },



                ],
                "optional": False
            },
            "payload": payload
        })

        # for database
        producer.poll(1)
        producer.produce('kafkaproject', data.encode('utf-8'))
        producer.flush()
        print(i+1)
        # time.sleep(3)


if __name__ == '__main__':
    ###########################

    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    print('Kafka Producer has been initiated...')
    #####################
    #create_sink_connector()

    mainproducer()

    #delete_sink_connector()
