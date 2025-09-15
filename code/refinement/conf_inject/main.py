import pandas as pd
import os
import re
import sys
import csv
import json
import logging
from inject import inject_config, clean_conf_file, inject_value_config
import time
sys.path.append("..")
from runCtestByConf.runLLM4Ctest import RunnerTest
from runCtestByConf.runCtest import run_ctest_by_conf
import config
import get_conf

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

info_path = {
    "hadoop": "hadoop_common.xlsx",
    "hdfs" :"hdfs.xlsx",
    "hbase": "",
    "zookeeper": "zookeeper.xlsx",
}

valid_conf_path = {
    "hadoop": "hcommon.xlsx",
    "zookeeper": "zookeeper.xlsx",
}

def get_LLM4Ctest_result(repo_name, is_valid=False):

   
    runner = RunnerTest(repo_name)
    
    conf_list = get_conf.get_conf_list(info_path[repo_name])
  
    conf_dict = {} 
    for conf in conf_list:
        conf_dict[conf] = []
    data = []
    for conf in conf_list:

        if is_valid:
            tag = "valid"
            inject_value = get_conf.get_valid_conf_value(conf_name=conf, inject_value__path=valid_conf_path[repo_name])
          
        else:
            tag = "invalid"
            inject_value = get_conf.get_misconf_value(conf_name=conf)
          
        for value in inject_value:

            param_value_pairs = {
                conf : value
            }
            inject_config(repo_name, param_value_pairs)
          
            test_result = runner.runTestByConf(conf)
           
            clean_conf_file(repo_name)
            for item in test_result:
                if item["test_info"] == "error test" or item["test_info"] == "failure test":
 
                    conf_dict[conf].append(value)
                    break
            data.append(test_result)

    timestamp = time.strftime('%Y%m%d_%H%M%S')
    store_path = f"{tag}_{repo_name}_{timestamp}.json"
    data_path = f"{tag}_{repo_name}_{timestamp}.json"
    with open(data_path, "w") as f:
            json.dump(data, f, indent=4)

    with open(store_path, "w") as f:
        json.dump(conf_dict, f, indent=4)
    

if __name__ == "__main__":
    repo_name = "hadoop"
    is_valid = False 
    get_LLM4Ctest_result(repo_name, is_valid)
