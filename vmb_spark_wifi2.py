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
    spark = SparkSession.builder.appName('VMB-wifi-score').enableHiveSupport().getOrCreate()
    mail_sender = MailSender() 
    hdfs_location = "http://njbbvmaspd11.nss.vzwnet.com:9870"

    # file ------------------------------------
    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'

    date_str = (date.today() - timedelta(2)).strftime("%Y-%m-%d")
    path = hdfs_title + "/user/maggie/wifi_score_v2.1/wifiScore_daily{}.json".format(date_str)
    df = spark.read.json(path).limit(10)
    
    df2=df.selectExpr("to_json(struct(*)) AS value")

    pulsar_topic = "persistent://cktv/wifi_score_v2-performance/VMAS-WiFi_score_v2-Performance"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-west-2-nonprod.verizon.com:6651/"

    #pulsar_topic = "persistent://cktv/5g-home-consolidated-performance/VMAS-5G-Home-Consolidated-Performance-Daily"
    #vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"

    key_path = "/usr/apps/vmas/cert/cktv/"
    key_path = "/usr/apps/vmas/script/ZS/VMB_key/wifi_certs/"
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
    """
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
    """
    #


    #-------------------------------------------------------------
    