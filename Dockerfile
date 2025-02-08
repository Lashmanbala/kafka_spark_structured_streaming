FROM apache/spark-py:latest

USER root

RUN pip install pyspark

WORKDIR /opt/spark/work-dir

