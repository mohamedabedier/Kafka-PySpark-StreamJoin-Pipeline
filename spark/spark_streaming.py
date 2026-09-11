from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,     
    TimestampType 
)

# open spark session
spark = SparkSession.builder \
    .appName("streaming2") \
    .master("local[*]") \
    .enableHiveSupport() \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# only warn or erorr
spark.sparkContext.setLogLevel("WARN")

# order schema
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_gender", StringType(), True),
    StructField("product_id", IntegerType(), True), 
    StructField("quantity", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("discount", FloatType(), True),
    StructField("order_time", TimestampType(), True)
])
# payment schema
payments_schema = StructType([
    StructField("payment_id", StringType(), True),  
    StructField("order_id", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("paid_amount", FloatType(), True),
    StructField("payment_time", TimestampType(), True)
])


orders_df = spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "stream_orders") \
.option("startingOffsets", "latest") \
.load()
# startingOffsets = latest = read -> process -> commit
payments_df = spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "stream_payments") \
.option("startingOffsets", "latest") \
.load()

# convert kafka value to json
orders_df = orders_df \
    .select(F.from_json(F.col("value").cast("string"), orders_schema).alias("data")) \
    .select("data.*")

payments_df = payments_df \
    .select(F.from_json(F.col("value").cast("string"), payments_schema).alias("data")) \
    .select("data.*")


# adding the expected amount to check if haker or not(if he payed less than expected)
orders_df = orders_df.withColumn(
    "Expected_amount",
    F.round((F.col("price") * F.col("quantity")) - F.col("discount"), 1)
)
# lazel round() 3alashan kan be5aly kolo Haker

# cleaning gender column
orders_df = orders_df.withColumn(
    "customer_gender",
    F.when(
        F.col("customer_gender").isin("M","m","male","1"),
        "Male"
    ).when(
        F.col("customer_gender").isin("F","f","female","0"),
        "Female"
    ).otherwise("7abibna bardo ya3m")
)



# watermark 5 sec bec, in the generate file I make the dely time 2-6 sec
# and I want to see the data that we will loss it


joined_stream = orders_df.alias("o").withWatermark("order_time", "5 seconds").join(
    payments_df.alias("p").withWatermark("payment_time", "5 seconds"),
    (F.col("o.order_id") == F.col("p.order_id")) &
    (F.col("p.payment_time") >= F.col("o.order_time")) &
    (F.col("p.payment_time") <= F.col("o.order_time") + F.expr("INTERVAL 5 SECONDS")),
    "inner"
)


# orders = orders_df.alias("orders").withWatermark("order_time", "5 seconds")
# payments = payments_df.alias("payments").withWatermark("payment_time", "5 seconds")

# joined_stream = orders.join(
#     payments,
#     F.col("orders.order_id") == F.col("payments.order_id") &
#     F.col("payments.payment_time") >= F.col("orders.order_time") &
#     F.col("payments.payment_time") <= F.col("orders.order_time") + F.expr("INTERVAL 5 SECONDS"),
#     "inner"
# )

joined_stream = joined_stream.withColumn(
    "is_haker",
    F.when(
        F.col("Expected_amount") > F.col("paid_amount"), "Haker"
        ).when(
        F.col("Expected_amount") < F.col("paid_amount"), "Dafa3 zeada 7elo"
        ).otherwise("Not Haker")
    )

joined_stream = joined_stream.withColumn(
    "leh_wala_3aleh",
    F.when(
        F.col("is_haker") == "Haker", F.concat(F.lit("3aleh ") , F.round(F.col("Expected_amount") - F.col("paid_amount"), 1).cast("string"))
        ).when(
        F.col("is_haker") == "Dafa3 zeada 7elo", F.concat(F.lit("leh ") , F.round(F.col("paid_amount") - F.col("Expected_amount"), 1).cast("string"))
        ).otherwise("0")
    )



# save in HDFS
query = joined_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path","/user/student/streaming2/final/data/") \
    .option("checkpointLocation","/user/student/streaming2/final/checkpoint/") \
    .trigger(processingTime="10 seconds") \
    .start()

# 2. write in console for testing
query_console = joined_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()


query.awaitTermination()


