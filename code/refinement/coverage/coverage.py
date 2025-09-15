import os
import sys
import subprocess
import re
import json
import time
import pandas as pd
from collections import defaultdict
from bs4 import BeautifulSoup


execute_path = {
    "hadoop" : "hadoop/hadoop-common-project/hadoop-common/",
    "hdfs" : "hadoop-hdfs-project/",
    "zookeeper" : "zookeeper-server/"
}


class Coverage:
    def __init__(self, repo_name):
        self.repo_name = repo_name
        self.current_path = os.path.dirname(os.path.abspath(__file__))

    
    def extract_methods_by_file1(self, text):
        result = defaultdict(list)
        
        lines = text.splitlines()
        current_file = None

        for line in lines:
            line = line.strip()
           
            path_match = re.match(r'^/*([\w\-/]+\.java)$', line)
            if path_match:
                current_file = path_match.group(1)
            elif current_file:
           
                method_match = re.match(r'^(public|private|protected|static|\s)*[\w\<\>\[\]]+\s+\w+\(.*', line)
                if method_match:
                  
                    line = re.sub(r'\s+', ' ', line.strip())  
                    result[current_file].append(line)

        return dict(result)

    def extract_methods_by_file(self, text):
        result = defaultdict(list)
        
        lines = text.splitlines()
        current_file = None
        current_method_lines = []
        collecting_method = False

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            path_match = re.match(r'^/*([\w\-/]+\.java)$', stripped)
            if path_match:
                current_file = path_match.group(1)
                continue

            if current_file:
                if collecting_method:
                    current_method_lines.append(line)
                    if ')' in line:
                        method_block = '\n'.join(current_method_lines).rstrip()
                        result[current_file].append(method_block)
                        current_method_lines = []
                        collecting_method = False
                else:
                    method_start_match = re.match(r'^(public|private|protected|static|\s)*[\w\<\>\[\]]+\s+\w+\s*\(.*', stripped)
                    if method_start_match:
                        current_method_lines = [line]
                        if ')' in line:
                            method_block = '\n'.join(current_method_lines).rstrip()
                            result[current_file].append(method_block)
                            current_method_lines = []
                        else:
                            collecting_method = True
        return dict(result)

    def extract_package_and_class(self, filepath):

        if 'src/main/java/' in filepath:
            relative_path = filepath.split('src/main/java/')[1]
        else:
            raise ValueError("路径中不包含 'src/main/java/'")


        parts = relative_path.strip().split('/')
        class_name_with_ext = parts[-1]
        package_parts = parts[:-1]

        package_name = '.'.join(package_parts)
        class_name = class_name_with_ext.replace('.java', '')

        return package_name, class_name


    def extract_method_lines_spans(self, html_path: str, signature_start: str) -> str:
        with open(html_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')

        code_block = soup.find('pre', {'class': 'source'})
        if not code_block:
            return ''


        raw_html = ''.join(str(child) for child in code_block.contents)
        html_lines = [line.rstrip('\n') for line in raw_html.split('\n')]

 
        text_lines = [line.strip() for line in code_block.get_text().splitlines()]


        target_lines = [line.strip() for line in signature_start.split('\n') if line.strip()]

        start_index = -1
        target_length = len(target_lines)
        if target_length == 0:
            return ''

        for i in range(len(text_lines) - target_length + 1):
            matched = True
            for j in range(target_length):
                if target_lines[j] not in text_lines[i + j]:
                    matched = False
                    break
            if matched:
                start_index = i
                break

        if start_index == -1:
            return ''

        method_html = []
        brace_count = 0

        for idx in range(start_index, start_index + target_length):
            if idx >= len(html_lines):
                break
            method_html.append(html_lines[idx])
            brace_count += text_lines[idx].count('{') - text_lines[idx].count('}')

        current_idx = start_index + target_length
        while current_idx < len(text_lines):
            if current_idx >= len(html_lines):
                break
            line_html = html_lines[current_idx]
            line_text = text_lines[current_idx]
            method_html.append(line_html)
            brace_count += line_text.count('{') - line_text.count('}')
            if brace_count == 0:
                break
            current_idx += 1

        return '\n'.join(method_html)    
        

    def get_report(self, execute_path, package_name, class_name, method_name):

        target_path = os.path.join(execute_path, f"target/site/jacoco/{package_name}/{class_name}.java.html")
     
       

        if os.path.exists(target_path):
            content = self.extract_method_lines_spans(target_path, method_name)
            return content
        else:
    
            return "None"
    


    def get_coverage_data(self, function_method):
     
        methods_by_file = self.extract_methods_by_file(function_method)

        file_num = len(methods_by_file)
        
        coverage_data = []

        for file_path, methods in methods_by_file.items():
      
            
            package_name, class_name = self.extract_package_and_class(file_path)

            for method in methods:
          
                report_content = self.get_report(execute_path[self.repo_name], package_name, class_name, method)

                if report_content == "None":
                    continue
                
                element = {
                    "file_path": file_path,
                    "method": method,
                    "report_content": report_content
                }
                coverage_data.append(element)


        os.chdir(self.current_path)
        with open(f"{self.repo_name}_coverage_data.json", "w") as f:
            json.dump(coverage_data, f, indent=4)

        return coverage_data


    def is_coverage_success(self, coverage_data):

        if "class=\"fc\"" in str(coverage_data):
            return True
        else:
            return False
