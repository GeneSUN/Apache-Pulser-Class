import pyspark.sql.functions as F
 
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import  asc, col, from_json, to_timestamp, window
 
 
 
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Streaming Pulsar Test") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
 
    
 
    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    file_path = hdfs_title + "/user/Charlotte/activation_report/Act_Report_2023-11-22"
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "True").load(file_path).limit(2)
    remove_column = ["hr", "mdn1", "mdn2", "mdn3", "mins", "sec"]
    column_name = [ name for name in df.columns if name not in remove_column]

    df = df.select(*column_name)\
            .withColumnRenamed("date1","outage_date_1")\
            .withColumnRenamed("date2","outage_date_2")\
            .withColumnRenamed("date3","outage_date_3")

    df2=df.selectExpr("to_json(struct(*)) AS value")
    df2.show()
    
    topic = "persistent://cktv/newuseractivation-anamoly/VMAS-NewUserActivation-Anamoly-Daily"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    #key_path = "/usr/apps/vmas/script/ZS/VMB_key/3868375/"
    key_path = "/usr/apps/vmas/cert/cktv/"
    cetpath = key_path + "cktv.cert.pem"
    keypath = key_path + "cktv.key-pk8.pem"
    capath = key_path + "ca.cert.pem"

    df2.write.format("pulsar") \
        .option("service.url", vmbHost_np) \
        .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls") \
        .option("pulsar.client.authParams",f"tlsCertFile:{cetpath},tlsKeyFile:{keypath}") \
        .option("pulsar.client.tlsTrustCertsFilePath",capath) \
        .option("pulsar.client.useTls","true") \
        .option("pulsar.client.tlsAllowInsecureConnection","false") \
        .option("pulsar.client.tlsHostnameVerificationenable","false") \
        .option("topic", topic) \
        .save()



#----------------------------------------------------------------------------------------------------------------------------
    """
    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    file_path = hdfs_title + "/user/Charlotte/activation_report/Act_Report_2023-11-22"
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "True").load(file_path).limit(2)
    remove_column = ["hr", "mdn1", "mdn2", "mdn3", "mins", "sec"]
    column_name = [ name for name in df.columns if name not in remove_column]

    df = df.select(*column_name)\
            .withColumnRenamed("date1","outage_date_1")\
            .withColumnRenamed("date2","outage_date_2")\
            .withColumnRenamed("date3","outage_date_3")

    df2=df.selectExpr("to_json(struct(*)) AS value")
    df2.show()
    
    topic = "persistent://cktv/5g-home-consolidated-performance/VMAS-5G-Home-Consolidated-Performance-Daily"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    key_path = "/usr/apps/vmas/cert/cktv/"
    cetpath = key_path + "cktv.cert.pem"
    keypath = key_path + "cktv.key-pk8.pem"
    capath = key_path + "ca.cert.pem"

    df2.write.format("pulsar") \
        .option("service.url", vmbHost_np) \
        .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls") \
        .option("pulsar.client.authParams",f"tlsCertFile:{cetpath},tlsKeyFile:{keypath}") \
        .option("pulsar.client.tlsTrustCertsFilePath",capath) \
        .option("pulsar.client.useTls","true") \
        .option("pulsar.client.tlsAllowInsecureConnection","false") \
        .option("pulsar.client.tlsHostnameVerificationenable","false") \
        .option("topic", topic) \
        .save()

    """

