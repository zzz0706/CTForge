import xml.etree.ElementTree as ET
import re

import pandas as pd
import os
import re
import json
import csv
import xml.etree.ElementTree as ET
import sys

from LLMPrompt import LLMPrompt
# from propagation_path import Path_Handler
import time
import ast

cflow_path = "hadoop_common.txt"
store_path = "hadoop.xlsx"

system_role = "You are a professional JAVA software engineer who understands the data propagation process and function call sequences in JAVA projects, and you can analyze the propagation path of configurations."


ask_prompt = """  
Below is the configuration information for HDFS 2.8.5. Please deduce brief configuration constraints and their usage within the code based on this information. Return only plain text .
The configuration information is as follows. For the configuration information, you need to understand the functionality of the configuration and its value constraints.
I will provide you with a propagation path of {conf_name} in {repo_name}.
// The propagation path below is the one you need to extract.
{propagation_path}
"""




class Data_flow:
    def __init__(self, cflow_path):
        self.sections = self.split_by_newline(cflow_path)
        self.llmCall = LLMPrompt("gpt-4o")
        self.repo_name = "hadoop2.8.5"

    def split_by_newline(self, filename):
        with open(filename, 'r') as file:

            content = file.read()

        pattern = r"Source:[\s\S]*?(?=Source:|$)"
        blocks = re.findall(pattern, content)

        return blocks
    
    def is_char_before_second_newline(self, data_flow, conf_name):

        first_newline_pos = data_flow.find('\n')
        if first_newline_pos == -1:
            return False  
        second_newline_pos = data_flow.find('\n', first_newline_pos + 1)
        if second_newline_pos == -1:
            return False 
        
        return conf_name in data_flow[:second_newline_pos]
    def extract_function(self, funclist): 
        pass
    
    def extract_methods_from_path(self, path_info):
 
        pattern = r'<([\w\d\$.<>]+):\s+([\w\d\$.<>]+)\s+([\w\d\$.<>]+)\(([\w\d\s\$.<>,\[\]]*)\)>'


        methods = re.findall(pattern, path_info)

        extracted_methods = []
        for method in methods:
            class_name, return_type, method_name, params = method
            extracted_methods.append(f"{class_name}: {method_name}({params})")

        return extracted_methods


    def search_by_conf(self, conf_name):
        data = []
        for section in self.sections:
            if self.is_char_before_second_newline(section, conf_name):
                data.append(section)
        return data

    def split_by_newline2(self, source_to_sink): 
   
        sections = re.split(r"\n{2,}", source_to_sink)

        sections = [section.strip() for section in sections if section.strip()]
        return sections
    
    def split_function_lists(self, function_lists) -> list:
  
        if function_lists[-1] != ']':
            function_lists = function_lists + ']]'
        
        return function_lists


    def remove_newline(self, string):
        return re.sub(r'\n', '', string)

    def function_list(self, conf_name, data_flow) -> list:
        function_list = []
        for i in data_flow:
            data = [] 
            for j in self.split_by_newline2(i):

                if conf_name not in j:
                    continue
             
                prompt = ask_prompt.replace("{conf_name}", conf_name).replace("{repo_name}", self.repo_name).replace("{propagation_path}", str(j))
               
                try :
                    func_list = self.llmCall.ask_LLM(prompt, system_role) 
                    func_list = self.remove_newline(func_list)
 
                except Exception as e:
                  
                  
                    func_list = self.llmCall.ask_LLM(prompt, system_role)
                    func_list = self.remove_newline(func_list)
                   
                data.append(func_list)
            function_list.append(data)
        return function_list
