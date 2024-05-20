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
    spark = SparkSession.builder.appName('VMB-wifi-score').getOrCreate()
    mail_sender = MailSender() 
    try:
        # file ------------------------------------
        hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
        date_str = (date.today() - timedelta(1)).strftime("%Y-%m-%d")

        models_vcg = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'XCI55AX','CR1000A','WNC-CR200A']
        models_vbg = ['FSNO21VA','ASK-NCM1100E','ASK-NCQ1338E']
        models = models_vcg + models_vbg

        df2 = spark.read.parquet(hdfs_title + f"/user/ZheS/wifi_score_v3/homeScore_dataframe/{date_str}")
        df2 = df2.withColumn( "dg_model_indiv", F.explode("dg_model")   )\
                .withColumn( "dg_model_indiv", F.explode("dg_model_indiv")   )\
                .select("serial_num",'mdn','cust_id','date','poor_rssi','poor_phyrate',"num_station",'home_score',"dg_model_indiv")\
                .dropDuplicates()\
                .fillna({"mdn":"0000000000","cust_id":"000000000"})

        df_vcg = df2.filter( col("dg_model_indiv").isin(models_vcg) )\
                    .selectExpr("to_json(struct(*)) AS value")

        pulsar_topic = "persistent://cktv/5g-home-router-wifi-scoring-performance/VMAS-5G-Home-Router-WIFI-Scoring-Performance-daily"
        vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
        vmbHost    = "pulsar+ssl://vmb-aws-us-east-1-prod.verizon.com:6651/"
        key_path = "/usr/apps/vmas/cert/cktv/"

        cetpath = key_path + "cktv.cert.pem"
        keypath = key_path + "cktv.key-pk8.pem"
        capath = key_path + "ca.cert.pem"
        # this is for product, all samples are uploaded for production
        df_vcg.write.format("pulsar")\
            .option("service.url", vmbHost)\
            .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls")\
            .option("pulsar.client.authParams",f"tlsCertFile:{cetpath},tlsKeyFile:{keypath}")\
            .option("pulsar.client.tlsTrustCertsFilePath",capath)\
            .option("pulsar.client.useTls","true")\
            .option("pulsar.client.tlsAllowInsecureConnection","false")\
            .option("pulsar.client.tlsHostnameVerificationenable","false")\
            .option("topic", pulsar_topic)\
            .save()
    except Exception as e:
        mail_sender.send( send_from ="vmb_spark_wifi@verizon.com", 
                         text = e,
                                    subject = f"vmb_spark_wifi prod failed !!! at {date_str}")
        
    try:
        # this is for nonprod, only 10 samples uploaded for test
        df_vcg.limit(10)\
            .write.format("pulsar") \
            .option("service.url", vmbHost_np) \
            .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls") \
            .option("pulsar.client.authParams",f"tlsCertFile:{cetpath},tlsKeyFile:{keypath}") \
            .option("pulsar.client.tlsTrustCertsFilePath",capath) \
            .option("pulsar.client.useTls","true") \
            .option("pulsar.client.tlsAllowInsecureConnection","false") \
            .option("pulsar.client.tlsHostnameVerificationenable","false") \
            .option("topic", pulsar_topic) \
            .save()
        job_nonprod = PulsarJob( pulsar_topic ,
                                    vmbHost_np, 
                                    cetpath, 
                                    keypath, 
                                    capath
                                )
        data = job_nonprod.setup_consumer()
        mail_sender.send(send_from ="vmb_spark_wifi@verizon.com",
                        subject = f"vmb_spark_wifi succeed at {date_str}",
                        text = data)
    except Exception as e:
        mail_sender.send( send_from ="vmb_spark_wifi@verizon.com", 
                text = e,
                        subject = f"vmb_spark_wifi nonprod failed !!! at {date_str}")











