import xml.etree.ElementTree as ET
import re

import pandas as pd
import os
import re
import json
import csv
import xml.etree.ElementTree as ET
import tqdm

import extract_cflow

from get_source_code import SourceCode

file_path = "core-default.xml"
store_path = "hadoop_common.xlsx"
cflow_path = "hadoop_common.txt"

tree = ET.parse(file_path)
root = tree.getroot()
print(root)

data_frame = []

data_flow = extract_cflow.Data_flow(cflow_path)

def get_dependency_info(conf_name):
    pass

num = 0
for child in root:
    
    num = num + 1

    data = ["", "", "", "", "[]", "[]"]
   
    is_values = False
    is_description = False
    is_over = 0
    for subchild in child:
  
        if subchild.tag == "name":
      
            data[0] = subchild.text
            is_name = True
            
        elif subchild.tag == "value":
  
            data[1] = subchild.text
            is_values = True
            
        elif subchild.tag == "description":

            data[2] = subchild.text
            is_description = True
        is_over = is_over + 1

        if (is_name and is_values and is_description) or is_over == len(child):
            retry_num = 0
            while True:
                data[3] = data_flow.search_by_conf(data[0])
                if str(data[3]) == "[]" or data[0] not in str(data[3]): 
                    data[4] = "[]"
                    data[5] = "[]"
                    break
              
                try:
                    data[4] = data_flow.function_list(data[0], data[3])
                    source_code = SourceCode()
                    data[5] = source_code.get_source_code_from_functionlist(str(data[4]))
                except Exception as e:
                   
                    data[4] = str(["error"])
                    data[5] = str(["error"])
              
                if retry_num == 3:
                    data[4] = "[]"
                    data[5] = "[]"
                    break
                break
    data_frame.append(data)



df = pd.DataFrame(data_frame, columns=["name", "values", "description", "summary", "source_code", "function_method"])
df.to_excel(store_path, index=False)
