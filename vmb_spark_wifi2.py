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
sys.path.append('/usr/apps/vmas/script/ZS') 
from MailSender import MailSender
from Pulsar_Class import PulsarJob
from Pulsar_Class import SparkToPulsar
import argparse
from datetime import datetime, timedelta, date
import pyspark.sql.functions as F
import types

def custom_nonprod_process_data(self,df):
            
    models_vcg = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'XCI55AX','CR1000A','WNC-CR200A']
    df = df.withColumn( "dg_model_indiv", F.explode("dg_model")   )\
            .withColumn( "dg_model_indiv", F.explode("dg_model_indiv")   )\
            .select("serial_num",'mdn','cust_id','date')\
            .dropDuplicates()\
            .fillna({"mdn":"0000000000","cust_id":"000000000"})\
            .filter( col("dg_model_indiv").isin(models_vcg) )\
            .selectExpr("to_json(struct(*)) AS value")
    
    df = df.limit(10)
    return df

def custom_prod_process_data(self,df):
            
    models_vcg = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'XCI55AX','CR1000A','WNC-CR200A']
    df = df.withColumn( "dg_model_indiv", F.explode("dg_model")   )\
            .withColumn( "dg_model_indiv", F.explode("dg_model_indiv")   )\
            .select("serial_num",'mdn','cust_id','date','poor_rssi','poor_phyrate',"num_station",'home_score',"dg_model_indiv")\
            .dropDuplicates()\
            .fillna({"mdn":"0000000000","cust_id":"000000000"})\
            .filter( col("dg_model_indiv").isin(models_vcg) )\
            .selectExpr("to_json(struct(*)) AS value")

    return df

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('VMB-wifi-score').getOrCreate()
    mail_sender = MailSender() 

    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    date_str = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
    file_path = hdfs_pd + f"/user/ZheS/wifi_score_v3/homeScore_dataframe/{date_str}"
    
    pulsar_topic = "persistent://cktv/5g-home-router-wifi-scoring-performance/VMAS-5G-Home-Router-WIFI-Scoring-Performance-daily"
    vmb_host_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    vmb_host    = "pulsar+ssl://vmb-aws-us-east-1-prod.verizon.com:6651/"

    try:    
        wifiScore_nonprod = SparkToPulsar(file_path, pulsar_topic, vmb_host_np) 
        wifiScore_nonprod.process_data = types.MethodType(custom_nonprod_process_data,wifiScore_nonprod)
        wifiScore_nonprod.run() 
        data = wifiScore_nonprod.consume_data()
        mail_sender.send(send_from ="vmb_spark_wifi@verizon.com",
                        subject = f"vmb_spark_wifi succeed at {date_str}",
                        text = data)

        wifiScore_prod = SparkToPulsar(file_path, pulsar_topic, vmb_host) 
        wifiScore_prod.process_data = types.MethodType(custom_prod_process_data,wifiScore_prod)
        wifiScore_prod.run() 

    except Exception as e:
        print(e)
        mail_sender.send( send_from ="vmb_spark_wifi@verizon.com", 
                text = e,
                        subject = f"vmb_spark_wifi nonprod failed !!! at {date_str}")


