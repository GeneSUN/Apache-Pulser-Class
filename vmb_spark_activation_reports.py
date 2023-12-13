from pulsar import Client, AuthenticationTLS, ConsumerType, InitialPosition
from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
from hdfs import InsecureClient
import os
import argparse 
from pyspark.sql.functions import ( 
    abs, avg, broadcast, col,concat, concat_ws, countDistinct, desc, exp, expr, explode, first, from_unixtime, 
    lpad, length, lit, max, min, rand, regexp_replace, round,struct, sum, to_json,to_date, udf, when, 
) 
import json
import sys 
sys.path.append('/usr/apps/vmas/script/ZS/SNAP') 
sys.path.append('/usr/apps/vmas/script/ZS') 
from MailSender import MailSender
from Pulsar_Class import PulsarJob, get_date_window
import argparse
from datetime import datetime, timedelta, date
from pyspark.sql.functions import  asc, col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,ArrayType,MapType,TimestampType,LongType
import pyspark.sql.functions as F

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('VMB-activation_reports_v5').enableHiveSupport().getOrCreate()
    mail_sender = MailSender() 
    hdfs_location = "http://njbbvmaspd11.nss.vzwnet.com:9870"

    # file ------------------------------------
    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'

    date_str = (date.today() - timedelta(7)).strftime("%Y-%m-%d")
    file_path = hdfs_title + f"/user/Charlotte/activation_report/Act_Report_{date_str}"

    df = spark.read.format("csv")\
                .option("header", "true")\
                .option("inferSchema", "True")\
                .load(file_path)
    
    remove_column = ["hr", "mdn1", "mdn2", "mdn3", "mins", "sec"]
    column_name = [ name for name in df.columns if name not in remove_column]

    df = df.select(*column_name)\
            .withColumnRenamed("date_1","outage_date_1")\
            .withColumnRenamed("date_2","outage_date_2")\
            .withColumnRenamed("date_3","outage_date_3")\
            .filter( (col("AVG_SCORE_1_3") < 85) )
    
    df2=df.selectExpr("to_json(struct(*)) AS value")

    pulsar_topic = "persistent://cktv/newuseractivation-anamoly/VMAS-NewUserActivation-Anamoly-Daily"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    vmbHost = "pulsar+ssl://vmb-aws-us-east-1-prod.verizon.com:6651/"
    key_path = "/usr/apps/vmas/cert/cktv/"
    #key_path = "/usr/apps/vmas/script/ZS/VMB_key/3868375/"
    #key_path = "/usr/apps/vmas/script/ZS/VMB_key/24656481/"
    cetpath = key_path + "cktv.cert.pem"
    keypath = key_path + "cktv.key-pk8.pem"
    capath = key_path + "ca.cert.pem"
    #"""
    df2.write.format("pulsar") \
        .option("service.url", vmbHost_np) \
        .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls") \
        .option("pulsar.client.authParams",f"tlsCertFile:{cetpath},tlsKeyFile:{keypath}") \
        .option("pulsar.client.tlsTrustCertsFilePath",capath) \
        .option("pulsar.client.useTls","true") \
        .option("pulsar.client.tlsAllowInsecureConnection","false") \
        .option("pulsar.client.tlsHostnameVerificationenable","false") \
        .option("topic", pulsar_topic) \
        .save()
    
    df2.write.format("pulsar") \
        .option("service.url", vmbHost) \
        .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls") \
        .option("pulsar.client.authParams",f"tlsCertFile:{cetpath},tlsKeyFile:{keypath}") \
        .option("pulsar.client.tlsTrustCertsFilePath",capath) \
        .option("pulsar.client.useTls","true") \
        .option("pulsar.client.tlsAllowInsecureConnection","false") \
        .option("pulsar.client.tlsHostnameVerificationenable","false") \
        .option("topic", pulsar_topic) \
        .save()
    #"""


    #-------------------------------------------------------------
    
    """
    schema = StructType([
        StructField("eventType", IntegerType(), True),
        StructField('location', MapType(StringType(),StringType()),True),
        StructField("timestamp" , LongType(), True),
        StructField("data" , StringType(), True)
        ])
    pulsar_source_df = spark \
        .readStream \
        .format("pulsar") \
        .option("service.url", vmbHost_np) \
        .option("admin.url", "https://vmb-aws-us-east-1-prod.verizon.com:8443") \
        .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls") \
        .option("pulsar.client.authParams",f"tlsCertFile:{cetpath},tlsKeyFile:{keypath}") \
        .option("pulsar.client.tlsTrustCertsFilePath",capath) \
        .option("pulsar.client.useTls","true") \
        .option("pulsar.client.tlsAllowInsecureConnection","false") \
        .option("pulsar.client.tlsHostnameVerificationenable","false") \
        .option("predefinedSubscription","test1234") \
        .option("topics", pulsar_topic) \
        .option("minPartitions","20") \
        .load()
    pulsar_source_df.printSchema()
 
    value_df = pulsar_source_df.selectExpr("CAST(value AS STRING)")
 
    value_df.printSchema()
    lines = value_df.select(from_json(col("value").cast("string"), schema).alias("parsed_value")).select(col("parsed_value.*"))
    df2 = lines.withColumn('date_parsed',F.from_unixtime(F.col('timestamp')/1000)).withColumn('latitude',F.col('location').latitude).withColumn('longitude',F.col('location').longitude).drop(F.col('location')).withColumn("hour",F.hour("date_parsed")).withColumn("date",F.to_date("date_parsed"))
    df2.printSchema()
    write_query = df2 \
        .writeStream \
        .format("json") \
        .partitionBy("date","hour") \
        .options(compression="gzip") \
        .option("checkpointLocation", "/test/mvs/chk-call-drop") \
        .option("path", "/test/mvs/call-drop") \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
        .start()
    
    write_query.show()
    # Stream Processing application will only terminate when you Manual Stop or Kill or Exception & shut down gracefully
    #write_query.awaitTermination()
    """