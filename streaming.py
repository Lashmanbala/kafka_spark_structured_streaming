from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round, to_json, struct, lit, current_timestamp, window, sum, count, when, expr
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("TestKafka") \
    .config("spark.sql.shuffle.partitions", 8) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                                   "org.postgresql:postgresql:42.5.0,"
                                   "io.delta:delta-core_2.12:2.4.0"
    ) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
    kafka_df.persist()   # persisting for mutiple writes
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
        output_df = eligible_df.select(col("customer_id").alias("key"), to_json(struct(*eligible_df.columns)).alias("value"))

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
                .option("path", "/opt/spark/data/parquet_output") \
                .save()
    
    finally:
        kafka_df.unpersist()


query = deduped_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_sinks) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/opt/spark/checkpoints/main") \
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

DELTA_WINDOW_PATH = "/opt/spark/data/delta/merchant_window_stats" # Local filesystem path inside the Spark container. Its mounted volume.

def upsert_window_batch(batch_df, batch_id):
    batch_df = batch_df.withColumn("batch_id", lit(batch_id))
 
    # update-mode batches with no changed windows are common, so skip the merge entirely rather than opening a Delta transaction for nothing.
    if batch_df.rdd.isEmpty():
        return
 
    if DeltaTable.isDeltaTable(spark, DELTA_WINDOW_PATH):

        delta_table = DeltaTable.forPath(spark, DELTA_WINDOW_PATH)
        delta_table.alias("t").merge(
            batch_df.alias("s"),
            "t.window_start = s.window_start AND t.merchant_id = s.merchant_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        # First batch ever: there's no Delta table so create it with a plain write.
        batch_df.write.format("delta").mode("overwrite").save(DELTA_WINDOW_PATH)
 

window_query = merchant_window_df.writeStream \
    .outputMode("update") \
    .foreachBatch(upsert_window_batch) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/opt/spark/checkpoints/window") \
    .start()

# Processing refunds
refund_schema = StructType([
    StructField("refund_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("refund_amount", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
])
 
refunds_kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "refunds_topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
 
refunds_parsed_df = refunds_kafka_df.selectExpr("CAST(value AS STRING) as value") \
    .withColumn("value_json", from_json(col("value"), refund_schema)) \
    .select("value_json.*")
 
refunds_watermarked_df = refunds_parsed_df.withWatermark("timestamp", "15 minutes")

# Recompute the eligible-transaction shape as a streaming transformation
eligible_txn_df = deduped_df \
    .where(col("customer_id").isNotNull() & col("timestamp").isNotNull()) \
    .filter((col("amount") > 500) & (col("merchant_id").isin("merch_1", "merch_3"))) \
    .withColumn("cashback", round(col("amount").cast("double") * 0.15, 2)) \
    .withColumnRenamed("timestamp", "txn_timestamp")  


# Join condition includes a time-range constraint (refund must land within 30 minutes after its transaction).
refunded_df = eligible_txn_df.alias("t").join(
    refunds_watermarked_df.alias("r"),
    expr("""
        t.transaction_id = r.transaction_id AND
        r.timestamp >= t.txn_timestamp AND
        r.timestamp <= t.txn_timestamp + interval 30 minutes
    """),
    "leftOuter"   # keep every eligible transaction, matched refund or not
).select(
    col("t.transaction_id"),
    col("t.customer_id"),
    col("t.merchant_id"),
    col("t.amount"),
    col("t.cashback"),
    col("t.txn_timestamp"),
    col("r.refund_id"),
    col("r.timestamp").alias("refund_timestamp")
).withColumn(
    "cashback_status",
    when(col("refund_id").isNotNull(), lit("REVERSED")).otherwise(lit("CONFIRMED"))
)


refund_join_query = refunded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/opt/spark/checkpoints/refund_join") \
    .start()

spark.streams.awaitAnyTermination()   # since we have multiple streaming queries
