# Cashback Streaming Project - Kafka and Spark streaming

## Overview

The requirement is to give cashback of specific percentile to the customer when the customer makes a payment more than certain amount and to specific merchants.

The cashback should be given in realtime. So, I used kafka and spark structured streaming in this project.

The financial data is recieved from a custom python application And I cofigured kafka producer in the same file producer.py.

Kafka is running in docker container with KRaft. Both controller and broker is configured in the same node. (Controller is for managing the cluster and metadata)

Spark is running in another docker container. The data is recieved from a kafka topic and being processed by spark.

And the eligible customer with the cashback details is sent to another kafka topic to give the cashback and also to a postgres db table to keep the record.

Incase of malformed incoming data, the error data will be stored in postgres db table for further processing.

Incase of any exceptional scenario such as unavailability of database, the unprocessed data will be saved as a parquet file in a directory for reprocessing. 

Postgres runs in a container and the tables'll be created while initializing the container with init.sql file in init_scripts directory.

## Setup
To setup this project locally, follow these steps

1. **Clone This Repositories:**
  ```bash
  mkdir kafka_spark_streaming
  cd kafka_spark_streaming
  git clone https://github.com/Lashmanbala/kafka_spark_streaming
  ```

2. **Install Docker and Docker compose**
 
3. **Edit the docker compose file with your values of volumes and environment variables**

4. **Run docker compose file**
   ```bash
    docker compose up
   ```
   Wait untill all the docker containers started and all the services are up
   
5. **Initialize kafka:**
   
   Get into the kafka container
   ```bash
    docker exec -it broker bash
    cd /opt/bitnami/kafka/bin
   ```
   Create kafka topics
   ```bash
    ./kafka-topics.sh --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --create --topic cashback_topic  
    ./kafka-topics.sh --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --create --topic eligible_customers_topic
    ```
   Subscribe to the output topic
    ```bash
    ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic eligible_customers_topic --from-beginning
    ```
6. **Publish to kafka topic:**

   Create a virtual environment and install required kafka libraries in local
   ```bash
    pip install -r requirements.txt
   ```
   Run the producer script to publish data to kafka
   ```bash
   python3 producer.py
   ```
8. **Initalize Spark:**
   
   Get into Spark container
   ```bash
    docker run -it --user root -p 4040:4040 --network kafka_spark_streaming_network_1 -v /home/ubuntu/kafka_spark_streaming:/opt/spark/work-dir spark /bin/bash
   ```

   Submit spark structured streaming application
   ```bash
    spark-submit --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.5.0 \
    /opt/spark/work-dir/streaming.py
   ```

10. **Check eligible_customers_topic in kafka and check eligible_customers table in postgres**

## Contact
For any questions, issues, or suggestions, please feel free to contact the project maintainer:

GitHub: [Lashmanbala](https://github.com/Lashmanbala)

LinkedIn: [Lashmanbala](https://www.linkedin.com/in/lashmanbala/)
