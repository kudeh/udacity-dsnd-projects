#!/bin/bash
docker exec -it spark-streaming-pipeline-spark-1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/sparkpyoptionalriskcalculation.py | tee  /home/workspace/spark/logs/optional-score.log
 