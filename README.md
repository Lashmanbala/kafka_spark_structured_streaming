# Cashback Streaming Project

## Overview

The requirement is to give cashback of specific percentile to the customer when the customer makes a payment more then certain amount and to specific merchants.

The cashback should be given in realtime. So, I used kafka and spark structured streaming in this project.

The financial data is recieved from a custom python application And I cofigured kafka producer in the same file producer.py.

Kafka is running in docker container with KRaft. Both controller and broker is configured in the same node. (Controller is for managing the cluster and metadata)

Spark is running in another docker container. The data is recieved from a kafka topic and being processed by spark.

And the eligible customer with the cashback details is sent to another kafka topic to give the cashback and also to a postgres db table to keep the record.

Incase of malformed incoming data, the error data will be stored in postgres db table for further processing.

Incase of any exceptional scenario such as unavailability of database, the unprocessed data will be saved as a parquet file in a directory for reprocessing. 

## Setup
To setup this project locally, follow these steps

1. **Clone This Repositories:**
  ```bash
  git clone https://github.com/Lashmanbala/kafka_spark_streaming
  ```

2. **Install Docker and Docker compose**
 
3. **Edit the docker compose file with your values of volumes and environment variables**

4. **Run docker compose file**
   ```bash
    docker compose up
   ```
   Wait untill all the docker containers started and all the services are up
