# kafka
**Work-flow** <br>
![workflow_image](https://github.com/pradipsapkotag/kafka/blob/main/kafka-workflow.png)  <br><br>

First run **`python random-amazon-products-scraper.py`** to scrape random amazon products id for api calls.
This will scrape random amazon products and store it in file **`random-items.txt`**.<br>
Then, run **`python api-calls.py`** to call api and store the response api data locally **`api-data-list.json`**<br><br>
In order ot get api call in real time. I have wrote a script to fet json data as of the rapidapi gives. For this you have to run **`python own-flask-api.py`**. This will repla with api data from local machine. <br><br>
Then after, its turn to run **`python kafkaProducer.py`** to produce data in diffetent topics. One topic(topic:kafkaproject) to store to database and one topic(topic:kafkaconsumer) for transformation and reproduce in different topic. From the topic **`kafkaproject`** produced data is stored in database with the help of sink connector. To initialize sink connector to store data in database python script is embedded in **`kafkaProducer.py`**.<br><br>
Then, run **`spark-submit kafkaConsumer`**. This will consume the data produced in the topic **`kafkaconsumer`** , transform them using pyspark and then reproduce data in different topic to store data into the database. The reproduced topics are   **`products`** ,   **`product_info`** adn **`features`**. This produced data will be stored to the database through the sink connector. <br><br>The sink connector is initialized by running the python script **`python conectors.py`**. This will initialize three sink-connectors for different three tables.