import pandas as pd
import os
import re
import csv


info_path = {
    "hadoop" : "hadoop_common.xlsx",
    "hdfs" : "hdfs.xlsx",
    "hbase" : "hbase.xlsx",
    "alluxio" : "alluxio.xlsx",
    "zookeeper" : "zookeeper.xlsx"
}


def get_conf_list(info_path):

    df = pd.read_excel(info_path)
    conf_list = df['name'].tolist()
    return conf_list

def get_misconf_value(conf_name):
    
    data = []
    inject_value__path = "injected_misconfig"
    
    with open(inject_value__path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        headers = next(reader)  # 读取表头

        for row in reader:         
            project = str(row[0])
            name = str(row[1])
            mis_conf = str(row[2])
            if name == conf_name:
                data.append(mis_conf)
                # print(mis_conf)
    return data

def get_valid_conf_value(conf_name, inject_value__path):
    data = []
   
    df = pd.read_excel(inject_value__path)

    for index, row in df.iterrows():         
        name = str(row['conf_name'])
        value1 = str(row['value1'])
        value2 = str(row['value2'])
        value3 = str(row['value3'])
        if name == conf_name:  
            if value1 != "" or value1 != "nan":
                data.append(value1)
            if value2 != "" or value2 != "nan":
                data.append(value2)
            if value3 != "" or value3 != "nan":
                data.append(value3)
            # print(mis_conf)
    return data

   