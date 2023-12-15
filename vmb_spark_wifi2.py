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
    path = hdfs_title + "/user/maggie/temp1.1/wifiScore_detail{}.json".format( (date.today() - timedelta(1)).strftime("%Y-%m-%d") )

    models = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'ASK-NCQ1338E', 'XCI55AX', 'CR1000A', 'FSNO21VA', 'ASK-NCM1100E']

    df_temp = spark.read.json(path)
    dfsh_all_Hsc = df_temp.groupby("serial_num","mdn","cust_id")\
                                        .agg(
                                        F.round(sum(df_temp.poor_rssi*df_temp.weights),4).alias("poor_rssi"),\
                                        F.round(sum(df_temp.poor_phyrate*df_temp.weights),4).alias("poor_phyrate"),\
                                        F.round(sum(df_temp.score*df_temp.weights),4).alias("home_score"),\
                                        max("dg_model").alias("dg_model"),\
                                        max("date").alias("date"),\
                                        max("firmware").alias("firmware")
                                        )
    df = dfsh_all_Hsc.select( "*",F.explode("dg_model").alias("dg_model_indiv") )\
                        .filter( col("dg_model_indiv").isin(models) )\
                        .drop("mdn","cust_id")
    
    p = hdfs_title+"/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{}/fixed_5g_router_mac_sn_mapping.csv"
    df_join = spark.read.option("header","true").csv(p.format(date_str))\
                .select( col("mdn_5g").alias("mdn"),
                         col("serialnumber").alias("serial_num"),
                         "cust_id"
                         )
    
    df2=df.join(df_join,"serial_num", "inner")\
            .selectExpr("to_json(struct(*)) AS value")

    pulsar_topic = "persistent://cktv/5g-home-router-wifi-scoring-performance/VMAS-5G-Home-Router-WIFI-Scoring-Performance-daily"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    key_path = "/usr/apps/vmas/cert/cktv/"

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
    """
    combo = "old_wifiscore_topic"
    if combo == "wifiscore": #does not works
        pulsar_topic = "persistent://cktv/wifi_score_v2-performance/VMAS-WiFi_score_v2-Performance"
        vmbHost_np = "pulsar+ssl://vmb-aws-us-west-2-nonprod.verizon.com:6651/"
        key_path = "/usr/apps/vmas/cert/cktv/wifi_certs/"
    elif combo == "snap": # works!
        #pulsar_topic = "persistent://cktv/5g-home-consolidated-performance/VMAS-5G-Home-Consolidated-Performance-Daily"
        vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
        pulsar_topic = "persistent://cktv/snap_event_enodeb_performance_drop-alert/VMAS-SNAP_event_eNodeB_performance_drop-Alert"
        key_path = "/usr/apps/vmas/cert/cktv/"
    elif combo == "wifiscore_oldkey": #does not works
        pulsar_topic = "persistent://cktv/wifi_score_v2-performance/VMAS-WiFi_score_v2-Performance"
        vmbHost_np = "pulsar+ssl://vmb-aws-us-west-2-nonprod.verizon.com:6651/"
        key_path = "/usr/apps/vmas/cert/cktv/"
    elif combo == "old_wifiscore_topic":
        pulsar_topic = "persistent://cktv/5g-home-router-wifi-scoring-performance/VMAS-5G-Home-Router-WIFI-Scoring-Performance-daily"
        vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
        key_path = "/usr/apps/vmas/cert/cktv/"
    """