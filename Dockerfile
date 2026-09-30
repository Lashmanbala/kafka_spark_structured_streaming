FROM apache/spark-py:v3.4.0

USER root

RUN pip install pyspark==3.4.0 delta-spark==2.4.0

WORKDIR /opt/spark/work-dir