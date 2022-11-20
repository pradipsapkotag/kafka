from confluent_kafka import Consumer
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, explode
import json

################
consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'kp'})
print('Kafka Consumer has been initiated...')
producer = Producer({'bootstrap.servers': 'localhost:9092'})
consumer.subscribe(['kafkaconsumer'])


# spark
spark = SparkSession\
    .builder\
    .appName("kafka-project")\
    .getOrCreate()


#######################################################################################
#    to table products table
#######################################################################################
def convertStockType(stk):
    try:
        if ('in stock' in stk.lower()):
            return True
        else:
            return False
    except:
        return


def pricehandler(price):
    try:
        price = price.split('$')
        return price[1]
    except:
        return 0


def str_maker(sth):
    return (str(sth))


def to_topic_product_info(data):
    db1 = spark.sparkContext.parallelize([data])
    df = spark.read.option("multiline", "true").json(db1)
    df.show()
    product_detail = df.select(['ASIN', 'average_rating', 'total_answered_questions',
                               'total_reviews', 'product_information.Manufacturer', 'small_description'])
    product_detail.show()
    product_detail = product_detail.toJSON().collect()

    for item in product_detail:
        try:
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
                                "field": "average_rating"
                        },
                        {
                                "type": "string",
                                "optional": True,
                                "field": "total_answered_questions"
                        },
                        {
                                "type": "string",
                                "optional": True,
                                "field": "total_reviews"
                        },
                        {
                                "type": "string",
                                "optional": True,
                                "field": "Manufacturer"
                        },
                        {
                                "type": "string",
                                "optional": True,
                                "field": "small_description"
                        }
                    ],
                    "optional": False
                },
                "payload": json.loads(item)
            })
            producer.poll(1)
            producer.produce('product_info', data.encode('utf-8'))
            producer.flush()
        except:
            pass


def to_topic_products_details(data):

    # for table products
    try:
        db1 = spark.sparkContext.parallelize(data)
        df = spark.read.option("multiline", "true").json(db1)
        df.show()
        products_details = df.select(
            ['ASIN', 'name', 'brand', 'pricing', 'availability_status', 'seller_name'])
        UDF = udf(lambda string: convertStockType(string), StringType())
        pUDF = udf(lambda string: pricehandler(string), StringType())
        products_details = products_details.withColumn('availability_status_new', UDF(col('availability_status'))).drop(
            'availability_status').withColumnRenamed('availability_status_new', 'availability_status')
        products_details = products_details.withColumn('price', pUDF(
            col('pricing'))).drop('pricing').withColumnRenamed('price', 'pricing')
        products_details = products_details.drop("availability_status")
        products_details.show()
        products_details = products_details.toJSON().collect()
        # produce data to topic products

        for item1 in products_details:
            # payload = payload_products_table(item)
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
                            "field": "brand"
                        },
                        {
                            "type": "string",
                            "optional": True,
                            "field": "pricing"
                        },
                        {
                            "type": "string",
                            "optional": True,
                            "field": "seller_name"
                        }
                    ],
                    "optional": False
                },
                "payload": json.loads(item1)
            })
            producer.poll(1)
            producer.produce('products', data.encode('utf-8'))
            producer.flush()

    except:
        pass

    # for table_product_info
    try:
        try:
            product_detail = df.select(['ASIN', 'average_rating', 'total_answered_questions',
                                        'total_reviews', 'product_information.Manufacturer', 'small_description'])

        except:
            product_detail = df.select(['ASIN', 'average_rating', 'total_answered_questions',
                                        'total_reviews', 'small_description'])

        product_detail.show()
        product_detail = product_detail.toJSON().collect()
        for item2 in product_detail:
            try:
                data = json.dumps({
                    "schema": {
                        "type": "struct",
                        "fields": [
                            {
                                "type": "string",
                                "optional": False,
                                "field": "ASIN"
                            },
                            {
                                "type": "string",
                                "optional": True,
                                "field": "average_rating"
                            },
                            {
                                "type": "string",
                                "optional": True,
                                "field": "total_answered_questions"
                            },
                            {
                                "type": "integer",
                                "optional": True,
                                "field": "total_reviews"
                            },
                            {
                                "type": "string",
                                "optional": True,
                                "field": "Manufacturer"
                            },
                            {
                                "type": "string",
                                "optional": True,
                                "field": "small_description"
                            }
                        ],
                        "optional": False
                    },
                    "payload": json.loads(item2)
                })
                producer.poll(1)
                producer.produce('product_info', data.encode('utf-8'))
                producer.flush()
            except:
                pass

    except:
        pass

    # for tabe_features
    try:
        newdf = df.select(['ASIN', 'feature_bullets'])
        new_df = newdf.withColumn('exploded_features', explode(col('feature_bullets'))).drop(
            'feature_bullets').withColumnRenamed('exploded_features', 'feature_bullets')
        ldf = new_df.toJSON().collect()
        for item3 in ldf:
            try:
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
                                    "field": "feature_bullets"
                                }
                        ],
                        "optional": False
                    },
                    "payload": json.loads(item3)
                })
                producer.poll(1)
                producer.produce('features', data.encode('utf-8'))
                producer.flush()
            except:
                pass
    except:
        pass
            


################
def main():
    dat = []
    print('inititlized \n\n\n')

    while True:
        msg = consumer.poll(1.0)  # timeout
        if msg is None:
            print("none\n\n\n")
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data = msg.value().decode('utf-8')
        print(data)
        dat.append(data)

        to_topic_products_details(dat)
        # to_topic_product_info(dat)
        dat = []

        # print(data)
    consumer.close()


if __name__ == '__main__':

    main()
