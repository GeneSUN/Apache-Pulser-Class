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

    # file ------------------------------------
    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    date_str = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
    #date_str = "2023-12-13"
    path = hdfs_title + "/user/maggie/temp1.1/wifiScore_detail{}.json".format( date_str )

    models_vcg = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'XCI55AX','CR1000A','WNC-CR200A']
    models_vbg = ['FSNO21VA','ASK-NCM1100E','ASK-NCQ1338E']
    models = models_vcg + models_vbg
    
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
    
    df2=df.join(df_join,"serial_num", "inner").drop("dg_model")
    df2.repartition(10).write.mode("overwrite").format("json")\
            .save( hdfs_title + "/user/ZheS/wifi_score_v2/vcg_vbg/" +  date_str)

    df_vcg = df2.filter( col("dg_model_indiv").isin(models_vcg) )\
                .selectExpr("to_json(struct(*)) AS value")

    pulsar_topic = "persistent://cktv/5g-home-router-wifi-scoring-performance/VMAS-5G-Home-Router-WIFI-Scoring-Performance-daily"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    key_path = "/usr/apps/vmas/cert/cktv/"

    cetpath = key_path + "cktv.cert.pem"
    keypath = key_path + "cktv.key-pk8.pem"
    capath = key_path + "ca.cert.pem"




    df_vcg.write.format("pulsar") \
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
    pulsar_topic = "persistent://cktv/5g-home-router-wifi-scoring-performance/VMAS-5G-Home-Router-WIFI-Scoring-Performance-daily"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    key_path = "/usr/apps/vmas/cert/cktv/"

    cetpath = key_path + "cktv.cert.pem"
    keypath = key_path + "cktv.key-pk8.pem"
    capath = key_path + "ca.cert.pem"
    job_nonprod = PulsarJob( pulsar_topic ,
                    vmbHost_np, 
                    cetpath, 
                    keypath, 
                    capath
                )
    data = job_nonprod.setup_consumer()
    mail_sender.send(text = data)

