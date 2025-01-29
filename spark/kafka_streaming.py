from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TestKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Read data from Kafka (update with your Kafka settings)
# in bootstrap server option, kafka is the container name in which kafka is running
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Process the streaming data
value_df = df.selectExpr("CAST(value AS STRING)")
query = value_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
