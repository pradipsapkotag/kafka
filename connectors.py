from api_keys import username, password

import requests



def creare_products_sink_connector():
    data = {
        "name": "products-table",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": f"jdbc:mysql://localhost:3306/Kafka?user={username}&password={password}",
            "topics": "products",
            "insert.mode.databaselevel": "true",
            "table.name.format": "products",
            "auto.offset.reset": "earliest",
            "auto.create": "true",
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
    print('\n\n\n')
    
    
    
def creare_features_sink_connector():
    data = {
        "name": "feature-table",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": f"jdbc:mysql://localhost:3306/Kafka?user={username}&password={password}",
            "topics": "features",
            "insert.mode.databaselevel": "true",
            "table.name.format": "features",
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
    print('\n\n\n')
    
    
    
def creare_product_info_sink_connector():
    data = {
        "name": "products-info-table",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": f"jdbc:mysql://localhost:3306/Kafka?user={username}&password={password}",
            "topics": "product_info",
            "insert.mode.databaselevel": "true",
            "table.name.format": "product_info",
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
    print('\n\n\n')
    
    
    
def delete_sink_connector():
    # delete conectror
    requests.delete("http://127.0.0.1:8083/connectors/products-table")
    requests.delete("http://127.0.0.1:8083/connectors/products-info-table")
    requests.delete("http://127.0.0.1:8083/connectors/feature-table")
    
    
def main():
    

    creare_product_info_sink_connector()
    # creare_products_sink_connector()
    creare_features_sink_connector()
    
    
    st = input("y to delete connectors\t")
    if(st == 'y'):
        delete_sink_connector()
    
main()
