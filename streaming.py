from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round, to_json, struct, lit, current_timestamp, window, sum, count
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("TestKafka") \
    .config("spark.sql.shuffle.partitions", 8) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# in bootstrap server option, broker is the container name in which kafka is running
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "cashback_topic") \
    .option('startingOffsets', 'earliest')\
    .option("failOnDataLoss", "false") \
    .load()

df_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])

value_df = kafka_df.selectExpr("CAST(value AS STRING)")   # converting value column from binary to string

json_df = value_df.withColumn('value_json', from_json(col('value'), df_schema)) 

parsed_df = json_df.select("value", "value_json.*")

watermarked_df = parsed_df.withWatermark("timestamp", "15 minutes")

deduped_df = watermarked_df.dropDuplicates(["transaction_id"])   # the producer resends the exact same event (same transaction_id) every DUPLICATE_EVERY transactions

def check_df(df):
    # Filtering error dataframe
    error_df = df.select("value").withColumn('event_timestamp', lit(current_timestamp())) \
                  .where(col("customer_id").isNull() | col("timestamp").isNull())
    
    # Filtering correct dataframe
    valid_df = df.where(col(".customer_id").isNotNull() & col("timestamp").isNotNull()) \
                    .drop("value") # droping the vraw value string in the df

    # getting the eligible customers who have paid more than 500 rs to either merch_1 or merch_3 and calculating the cashback 15%
    eligible_df = valid_df.filter((col('amount') > 500) & (col('merchant_id').isin('merch_1', 'merch_3'))) \
                        .withColumn('cashback', round(col('amount').cast('double') * 0.15, 2)) \
                        .select(col("customer_id"),col("amount"),col("cashback"),col("merchant_id"),col("timestamp"),col("payment_method"))
    
    return error_df, eligible_df

def postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_db:5432/cashback_db") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password",  "postgres123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def write_to_sinks(kafka_df, batch_id):    # these args'll be internally passed by foreachBatch function
    try:
        error_df_raw, eligible_df = check_df(kafka_df)

        # adding batch_id with the error df
        error_df = error_df_raw.withColumn('batch_id', lit(batch_id))

        # writing error df into errors table
        table_name = 'error_table'
        postgres(error_df, table_name)

        # writing eligible df into eligible customers table
        table_name = 'eligible_customers'
        postgres(eligible_df, table_name)

        # Kafka takes only string. And the clm name should be value
        # converting the df into json string with clm name as 'value'
        output_df = eligible_df.select(to_json(struct(*eligible_df.columns)).alias("value"))

        output_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:9092") \
            .option("topic", "eligible_customers_topic") \
            .mode("append") \
            .save()

    except Exception as e:
        # incase of exceptional scenario such as disconnection  with databse the unprocessed data is saved as a parquet file
        print(e)
        kafka_df.write \
                .format("parquet") \
                .mode("append") \
                .option("path", "./parquet_output") \
                .save()


query = deduped_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_sinks) \
    .option("checkpointLocation", "./kafka_checkpoints") \
    .start()

# Aggregation 
merchant_window_df = deduped_df \
    .where(col("customer_id").isNotNull() & col("timestamp").isNotNull()) \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("merchant_id")
        ) \
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("txn_count")
        ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("merchant_id"),
        col("total_amount"),
        col("txn_count")
        )

window_query = merchant_window_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "./window_checkpoints") \
    .start()

spark.streams.awaitAnyTermination()   # since we have multiple streaming queries
