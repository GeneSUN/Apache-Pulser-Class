from pyspark.sql import SparkSession 
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField, FloatType) 
from pyspark.sql.functions import ( 
    col, from_json
) 

import sys 

from datetime import datetime, timedelta 
 
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Streaming Pulsar Test2") \
        .master("spark://njbbepapa1.nss.vzwnet.com:7077") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    pulsar_topic = "persistent://cktv/5g-home-router-wifi-scoring-performance/VMAS-5G-Home-Router-WIFI-Scoring-Performance-daily"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    vmbHost    = "pulsar+ssl://vmb-aws-us-east-1-prod.verizon.com:6651/"
    key_path = "/usr/apps/vmas/cert/cktv/"

    cetpath = key_path + "cktv.cert.pem"
    keypath = key_path + "cktv.key-pk8.pem"
    capath = key_path + "ca.cert.pem"

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

    # define schema of reading table

    schema = StructType([
        StructField("serial_num", StringType(), True),
        StructField("mdn", StringType(), True),
        StructField("cust_id", StringType(), True),
        StructField('date', StringType(),True),
        StructField("poor_rssi" , FloatType(), True),
        StructField("poor_phyrate" , FloatType(), True),
        StructField("home_score" , FloatType(), True),
        StructField("dg_model_indiv" , StringType(), True),

        ])
    lines = value_df.select(from_json(col("value").cast("string"), schema).alias("parsed_value")).select(col("parsed_value.*"))
    #lines.printSchema()

    write_query = lines \
        .writeStream \
        .format("json") \
        .option("checkpointLocation", "/user/ZheS/vmb_spark/chk-call-drop") \
        .option("path", "/user/ZheS/vmb_spark/try") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
 
    write_query.awaitTermination()

