from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round, to_json, struct
from pyspark.sql.types import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TestKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# Read data from Kafka (update with your Kafka settings)
# in bootstrap server option, broker is the container name in which kafka is running
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "test-topic") \
    .option('startingOffsets', 'earliest')\
    .option("failOnDataLoss", "false") \
    .load()

# Process the streaming data
value_df = df.selectExpr("CAST(value AS STRING)")

## Input Schema
df_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("merchant_id", StringType(), True)
])

json_df = value_df.withColumn('value_json', from_json(col('value'), df_schema))\
                .selectExpr('value_json.*')

eligible_df = json_df.filter((col('amount') > 500) & 
                             (col('merchant_id').isin('merch_1', 'merch_3'))) \
                     .withColumn('cashback', round(col('amount').cast('double') * 0.15, 2)) \
                     .select(col("customer_id"),col("amount"),col("cashback"),col("merchant_id"),col("timestamp"))


def write_to_postgres(batch_df, batch_id):    # these args'll be internally passed by foreachBatch function

    # writing into kafka topic

    # Kafka takes only string. And the clm name should be value
    # converting the df into json string with clm name as 'value'
    kafka_df = batch_df.select(to_json(struct(*eligible_df.columns)).alias("value"))

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("topic", "eligible_cutomers_topic") \
        .mode("append") \
        .save()
    
    print(f"Batch {batch_id} written to Kafka")
    
    # writing into postgres
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_db:5432/cashback_db") \
        .option("dbtable", "eligible_customers") \
        .option("user", "postgres") \
        .option("password",  "postgres123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"Batch {batch_id} written to PostgreSQL")

    # writing as a parquet
    batch_df.write \
        .format("parquet") \
        .mode("append") \
        .option("path", "./parquet_output") \
        .save()
    
    print(f"Batch {batch_id} written to Parquet")

query = eligible_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "./kafka_checkpoints") \
    .start()

query.awaitTermination()