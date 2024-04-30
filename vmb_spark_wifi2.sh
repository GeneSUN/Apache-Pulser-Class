#!/bin/bash 

export HADOOP_HOME=/usr/apps/vmas/hadoop-3.3.6
export SPARK_HOME=/usr/apps/vmas/spark
export PYSPARK_PYTHON=/usr/apps/vmas/anaconda3/bin/python3 
echo "RES: Starting===" 
echo $(date) 
/usr/apps/vmas/spark/bin/spark-submit \
--master spark://njbbepapa1.nss.vzwnet.com:7077 \
--conf "spark.sql.session.timeZone=UTC" \
--conf "spark.driver.maxResultSize=2g" \
--conf "spark.dynamicAllocation.enabled=false" \
--num-executors 20 \
--executor-cores 2 \
--total-executor-cores 40 \
--executor-memory 2g \
--driver-memory 16g \
--packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.4.0.2 \
/usr/apps/vmas/script/ZS/spark_vmb/vmb_spark_wifi2.py \
> /usr/apps/vmas/script/ZS/spark_vmb/vmb_spark_wifi2.log

 
 

