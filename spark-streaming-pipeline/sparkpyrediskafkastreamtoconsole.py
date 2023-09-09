from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType,
    DateType,
)

# Before Spark 3.0.0, schema inference is not automatic

# create a StructType for the Kafka redis-server topic which has all changes made to Redis
redis_server_schema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField(
            "zSetEntries",
            ArrayType(
                StructType(
                    [
                        StructField("element", StringType()),
                        StructField("score", StringType()),
                    ]
                )
            ),
        ),
    ]
)

# create a StructType for the Customer JSON that comes from Redis
customer_schema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)

# create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis
customer_stedi_event_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate", StringType()),
    ]
)

# create a spark application object
spark = SparkSession.builder.appName("stedi-redis-kafka-connect-pipeline").getOrCreate()

# set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# read a streaming dataframe from the Kafka topic `redis-server` as the source
redis_raw_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# cast the value column in the streaming dataframe as a STRING
redis_stream_df = redis_raw_stream_df.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# create temp view from value field as json of redis-server topic
redis_stream_df.withColumn("value", from_json("value", redis_server_schema)).select(
    col("value.*")
).createOrReplaceTempView("RedisSortedSet")

# extract customer base64 encoded data from the temp view
encoded_customer_data_df = spark.sql(
    "select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet"
)

# decode the customer data
decoded_customer_data_df = encoded_customer_data_df.withColumn(
    "encodedCustomer", unbase64(encoded_customer_data_df.encodedCustomer).cast("string")
)

# parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decoded_customer_data_df.withColumn(
    "encodedCustomer", from_json("encodedCustomer", customer_schema)
).select(col("encodedCustomer.*")).createOrReplaceTempView("CustomerRecords")

#  extract email and birthday from df
emailAndBirthDayStreamingDF = spark.sql(
    "SELECT email, birthDay FROM CustomerRecords WHERE email is not null AND birthDay is not null"
)

# from the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)
# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear"),
)

# sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
#
# The output should look like this:
# +--------------------+-----
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----
emailAndBirthYearStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct
