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
sys.path.append('/usr/apps/vmas/scripts/ZS/OOP_dir') 
from MailSender import MailSender
from Pulsar_Class import PulsarJob, get_date_window
import argparse
from datetime import datetime, timedelta, date

import time 
 
class ConvertDfJson: 
    global hdfs_title, tickets_list, features_list, alert_feature_list,selected_feature
    hdfs_title = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    tickets_list = ["nrb_ticket_counts","w360_ticket_counts"]
    #features_list = ['sip_dc_rate', 'context_drop_rate', 'bearer_drop_voice_rate', 'bearer_setup_failure_rate', 'avgconnected_users', 'dl_data_volume', 'uptp_user_perceived_throughput', 'packet_retransmission_rate', 'packet_loss_rate', 'rrc_setup_failure', 'bearer_drop_rate']
    features_list =  ['sip_dc_rate', 'rrc_setup_failure', 'bearer_drop_rate',"rtp_gap",'avgconnected_users', 'dl_data_volume','uptp_user_perceived_throughput']
    
    alert_feature_list = [  f"{feature}_alert"  for feature in features_list]
    selected_feature = ["enodeb","day","event_day","site","region","abnormal_kpi"] + features_list + tickets_list + alert_feature_list
    
    def __init__(self, date_val, read_path, hdfs_url, save_path): 
        self.spark = SparkSession.builder.appName("PulsarJob").getOrCreate() 
        self.date_val = date_val
        self.read_path = read_path.format(self.date_val)
        self.hdfs_url = hdfs_url
        self.save_path = save_path
        self.table = self.spark.read.option("recursiveFileLookup", "true")\
                                .option("header", "true").csv(hdfs_title + self.read_path)\
                                .select(selected_feature)\
                                .filter(col("has_abnormal_kpi") == 1)
        self.table2 = self.process_data()
        self.dict_table = self.table2.toPandas()\
                                    .to_dict(orient = "records")
        self.text_data_bytes = self.filter_fun()
        self.save_json()
    

    def read_table(self, read_path = None, target_date_str = None, days_before = 3): 

        if read_path is None:
            read_path = self.read_path
        if target_date_str is None:
            target_date_str = self.date_val
        
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d") 
        start_date = target_date - timedelta(days=days_before) 
        file_paths = [ 
            f"{read_path}/{target_date_str}_tickets_Post_feature_maintained_{(start_date + timedelta(days=i)).strftime('%Y-%m-%d')}.csv" 
            for i in range(days_before + 1) 
        ] 
    
        df = spark.read.option("header", "true").csv(file_paths) 
    
        return df 

    def process_data(self, table = None): 
     
        if table is None:
            table =  self.table
        
        struct_col = struct(*[table[col] for col in features_list])  
        struct_features_alert = struct(*[table[col] for col in alert_feature_list])  
 
        joined_table = table.withColumnRenamed("day", "alert_day")\
            .withColumnRenamed("abnormal_kpi", "abnormal_kpi_count")\
            .withColumn("vendor", lit("samsung"))\
            .withColumn("features", to_json(struct_col))\
            .withColumn("features_alert", to_json(struct_features_alert))\
            .withColumn("link", concat(lit("https://sas.vzbi.com/eNodeBPerformance?enb="), col("enodeb")))\
            .drop(*features_list)\
            .drop(*alert_feature_list) 

        return joined_table 

    def filter_features(self, features_str, features_alert_str): 
        features_data = json.loads(features_str)  
        features_alert_data = json.loads(features_alert_str)  

        filtered_features_data = {  
            key: value  
            for key, value in features_data.items()  
            if features_alert_data[f"{key}_alert"] != '0'  
        }  

        return filtered_features_data 

    def filter_fun(self, dict_table = None):
        if dict_table is None:
            dict_table = self.dict_table
        filtered_data_list = [ 
            { 
            **data, 
            'features': self.filter_features(data['features'], data['features_alert']) 
            } 
                for data in dict_table 
        ]
        text_data_bytes = json.dumps(filtered_data_list, indent=2).encode('utf-8') 
        
        for data in filtered_data_list:
            data.pop('features_alert', None)
        
        return text_data_bytes

    def save_json(self, hdfs_url=None, save_path = None):
        if hdfs_url is None:
            hdfs_url = self.hdfs_url
        if save_path is None:
            save_path = self.save_path
        hdfs_client = InsecureClient(self.hdfs_url) 
        with hdfs_client.write(save_path, overwrite=True) as writer: 
            writer.write(self.text_data_bytes) 

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('Push_SNAP_abnormal_enodeb_to_vmb')\
                        .master("spark://njbbvmaspd11.nss.vzwnet.com:7077")\
                        .config("spark.ui.port","24040")\
                        .enableHiveSupport().getOrCreate()
    mail_sender = MailSender() 


    # file ------------------------------------
    hdfs_location = "http://njbbvmaspd11.nss.vzwnet.com:9870"
    date_val = ( datetime.now().date() - timedelta(1) ).strftime("%Y-%m-%d")


    read_path = "/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_Post_tickets_Feature_Date/{}_tickets_Post_Feature_of_Enodeb"
    save_path = f"/user/ZheS/SNAP_Enodeb/VMB/VMAS-Post-SNAP-Maintenance-Alert/json_abnormal_enodeb-{date_val}.json"
    inst = ConvertDfJson(date_val,
                            read_path,
                            hdfs_location,
                            save_path)

    pulsar_topic = "persistent://cktv/post-snap-maintenance-alert/VMAS-Post-SNAP-Maintenance-Alert"
    vmbHost_np = "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    vmbHost = "pulsar+ssl://vmb-aws-us-east-1-prod.verizon.com:6651/"
    
    key_folder = "/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/"
    cetpath = key_folder + "cktv.cert.pem"
    keypath = key_folder + "cktv.key-pk8.pem"
    capath = key_folder + "ca.cert.pem"
    """


    """
    try:
        
        job_prod = PulsarJob( pulsar_topic ,
                            vmbHost, 
                            cetpath, 
                            keypath, 
                            capath,
                            save_path, 
                            hdfs_location
                        )
        job_prod.setup_producer()
        
        job_prod = PulsarJob( pulsar_topic ,
                        vmbHost, 
                        cetpath, 
                        keypath, 
                        capath,
                        save_path, 
                        hdfs_location
                    )
        data = job_prod.setup_consumer()
        if data is None:
            mail_sender.send(text=f"Failled!!!!!")
        else:
            mail_sender.send(subject="vmb_post_snap script Successful",
                            text=f"script at /usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/n {data}") 
        #data = job1.setup_consumer(); mail_sender.send(text=f"script at /usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/n {data}") 
    except Exception as e:
        
        mail_sender.send(send_from="sassupport@verizon.com", 
                        send_to=["zhe.sun@verizonwireless.com"], 
                        subject="vmb_post_snap consumer failed", 
                        cc=[], 
                        text=f"script at /usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/n {e}") 

    start_time = "11:30" 
    end_time = "20:30" 
    start_datetime = datetime.strptime(start_time, "%H:%M") 
    end_datetime = datetime.strptime(end_time, "%H:%M") 

    while True: 
        time.sleep(300)  # Optional: Adjust the sleep time to balance responsiveness and resource usage 
        current_time = datetime.now().time() 
        if start_datetime.time() <= current_time <= end_datetime.time(): 
            job_prod = PulsarJob( pulsar_topic ,
                        vmbHost, 
                        cetpath, 
                        keypath, 
                        capath,
                        save_path, 
                        hdfs_location
                    )
            job_prod.setup_producer()
        else:
            break
    
    job_prod.move_file_to_archive()
    """
    """