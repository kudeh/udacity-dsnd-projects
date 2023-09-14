# Data Streaming Nanodegree Projects
My Project Solutions From [Udacity Data Streaming Nanodegree Program](https://www.udacity.com/course/data-streaming-nanodegree--nd029).

## 1. [Data Ingestion with Kafka & Kafka Streaming](https://github.com/kudeh/udacity-dsnd-projects/tree/main/kafka-streaming-pipeline)
Worked on ingesting data into kafka and stream processing on kafka topics.

* **Tasks Completed:**
    * Set up Kafka Topics and Schemas
    * Set up postgres kafka connector
    * Implement Kafka Producers using kafka-client and rest proxy
    * Implement Kafka Consumers/Pipelines with faust and ksql

* **Concepts Learned:**
    * Apache Kafka Infrastructure
    * Data Schemas and Apache Avro
    * Kafka Connect and REST Proxy
    * Stream Processing Fundamentals
    * Stream Processing with Faust
    * KSQL

* **Core Technologies Used:**
    * Python (confluent-kafka, faust)
    * Apache Kafka


## 2. [Streaming API Development and Documentation](https://github.com/kudeh/udacity-dsnd-projects/tree/main/spark-streaming-pipeline)
Worked on processing apache kafka data streams using pyspark streaming APIs

* **Tasks Completed:**
    * Implement spark streaming pipelines
    * Consume from apache kafka topic
    * decode base64 encoded json data from stream
    * perform transformations and aggregation on data
    * join streams and output to new kafka topic or console

* **Concepts Learned:**
    * Spark Dataframes, Views and Spark SQL
    * Reading binary, json and base64 encoded data from kafka streams
    * Joining streams

* **Core Technologies Used:**
    * Python (pyspark)
    * Apache Spark
    * Apache Kafka
    * Redis
