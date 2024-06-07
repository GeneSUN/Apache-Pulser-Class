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
import pyspark.sql.functions as F

class SparkToPulsar: 

    def __init__(self, file_path, pulsar_topic, vmb_host): 
        self.file_path = file_path 
        self.pulsar_topic = pulsar_topic 
        self.vmb_host = vmb_host 
        self.main_path = "/usr/apps/vmas/cert/cktv/"
        self.cert_path = self.main_path + "cktv.cert.pem"
        self.key_path = self.main_path + "cktv.key-pk8.pem"
        self.ca_path = self.main_path + "ca.cert.pem"

    def read_data(self): 
        df = spark.read.parquet(self.file_path) 
        return df 
 
    def process_data(self, df): 
        
        models_vcg = ['ASKNCQ138', 'ASKNCQ138FA', 'XC1X5X', 'CR100EA', 'WNC-CR200A'] 
        df = df.withColumn( "dg_model_indiv", F.explode("dg_model")   )\
                .withColumn( "dg_model_indiv", F.explode("dg_model_indiv")   )\
                .select("serial_num",'mdn','cust_id','date','poor_rssi','poor_phyrate',"num_station",'home_score',"dg_model_indiv")\
                .dropDuplicates()\
                .fillna({"mdn":"0000000000","cust_id":"000000000"})\
                .filter( col("dg_model_indiv").isin(models_vcg) )\
                .selectExpr("to_json(struct(*)) AS value")
        
        df = df.limit(10)
        return df 

    def write_data(self, df): 

        df.write.format("pulsar")\
            .option("service.url", self.vmb_host)\
            .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls")\
            .option("pulsar.client.authParams",f"tlsCertFile:{self.cert_path},tlsKeyFile:{self.key_path}")\
            .option("pulsar.client.tlsTrustCertsFilePath",self.ca_path)\
            .option("pulsar.client.useTls","true")\
            .option("pulsar.client.tlsAllowInsecureConnection","false")\
            .option("pulsar.client.tlsHostnameVerificationenable","false")\
            .option("topic", self.pulsar_topic)\
            .save()

    def run(self): 
        df = self.read_data()
        df = self.process_data(df) 
        df.show()
        self.write_data(df) 

    def consume_data(self):
        from Pulsar_Class import PulsarJob
        job_nonprod = PulsarJob( self.pulsar_topic ,
                                    self.vmb_host, 
                                    self.cert_path , 
                                    self.key_path, 
                                    self.ca_path
                                )
        data = job_nonprod.setup_consumer()

        return data

if __name__ == "__main__":

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
        wifiScore_nonprod.run() 
        data = wifiScore_nonprod.consume_data()

        mail_sender.send(send_from ="vmb_spark_wifi@verizon.com",
                        subject = f"vmb_spark_wifi succeed at {date_str}",
                        text = data)
        
        wifiScore_prod = SparkToPulsar(file_path, pulsar_topic, vmb_host) 
        wifiScore_prod.run() 

    except Exception as e:
        mail_sender.send( send_from ="vmb_spark_wifi@verizon.com", 
                text = e,
                        subject = f"vmb_spark_wifi nonprod failed !!! at {date_str}")


