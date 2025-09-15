import os
import re
import json
import pandas as pd
from collections import defaultdict
from bs4 import BeautifulSoup

from .. import config

# get jacoco coverage; tests must pass
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
            # determine if line denotes a new path (may have leading slashes)
            path_match = re.match(r'^/*([\w\-/]+\.java)$', line)
            if path_match:
                current_file = path_match.group(1)
            elif current_file:
                # check whether line is a function signature
                method_match = re.match(r'^(public|private|protected|static|\s)*[\w\<\>\[\]]+\s+\w+\(.*', line)
                if method_match:
                    # join multi-line parameters
                    line = re.sub(r'\s+', ' ', line.strip())  # remove extra spaces
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

            # check whether line is a path
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
        # remove prefix up to src/main/java/
        if 'src/main/java/' in filepath:
            relative_path = filepath.split('src/main/java/')[1]
        else:
            raise ValueError("path does not contain 'src/main/java/'")

        # separate package path and class name
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

        # extract raw HTML lines (retain tags)
        raw_html = ''.join(str(child) for child in code_block.contents)
        html_lines = [line.rstrip('\n') for line in raw_html.split('\n')]

        # extract plain text lines (for logic)
        text_lines = [line.strip() for line in code_block.get_text().splitlines()]

        # split multiline signature and filter empty lines
        target_lines = [line.strip() for line in signature_start.split('\n') if line.strip()]

        # search for matching start line
        start_index = -1
        target_length = len(target_lines)
        if target_length == 0:
            return ''

        # iterate through possible start lines
        for i in range(len(text_lines) - target_length + 1):
            matched = True
            for j in range(target_length):
                # check if target line exists in current line
                if target_lines[j] not in text_lines[i + j]:
                    matched = False
                    break
            if matched:
                start_index = i
                break

        if start_index == -1:
            return ''

        # collect code and track braces
        method_html = []
        brace_count = 0

        # 1. collect signature lines (with HTML tags)
        for idx in range(start_index, start_index + target_length):
            if idx >= len(html_lines):
                break
            method_html.append(html_lines[idx])
            brace_count += text_lines[idx].count('{') - text_lines[idx].count('}')

        # 2. collect following lines until braces close
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
        """
            Get jacoco report for class_name_java.html
        """
        target_path = os.path.join(execute_path, f"target/site/jacoco/{package_name}/{class_name}.java.html")
        # print(f">>>[get_report] execute_path: {execute_path}")
        # print(f">>>[get_report] target_path: {target_path}")    
        

        if os.path.exists(target_path):
            content = self.extract_method_lines_spans(target_path, method_name)
            return content
        else:
            # print(f">>>[jacoco] File {target_path} does not exist.")
            return "None"
    


    def get_coverage_data(self, function_method):
        """
            Get coverage data
        """
        methods_by_file = self.extract_methods_by_file(function_method)

        file_num = len(methods_by_file)
        
        coverage_data = []

        for file_path, methods in methods_by_file.items():
            # print(f">>>>>file_path:{file_path}")
            
            package_name, class_name = self.extract_package_and_class(file_path)

            for method in methods:
                # print(f"{method}")
                report_content = self.get_report(config.execute_path[self.repo_name], package_name, class_name, method)

                # print(f">>>[jacoco] report_content: {report_content}")

                if report_content == "None":
                    # print(f">>>[jacoco] File {file_path} does not exist.")
                    continue
                
                element = {
                    "file_path": file_path,
                    "method": method,
                    "report_content": report_content
                }
                coverage_data.append(element)

        # write to json file
        os.chdir(self.current_path)
        with open(f"{self.repo_name}_coverage_data.json", "w") as f:
            json.dump(coverage_data, f, indent=4)

        return coverage_data


    def is_coverage_success(self, coverage_data):
        """ 
            determine coverage
        """
        if "class=\"fc\"" in str(coverage_data):
            return True
        else:
            return False

    def get_source_code(self):
        """ 
            get source code
        """
        path = config.info_path["hadoop"]
        df = pd.read_excel(path)  # read configuration information
        for index, row in df.iterrows():

            name = row['name']
            values = row['values']
            description = row['description']
            dependency_info = row['dependency']
            data_flow_summary = row['data_flow_summary']
            function_signature = row['function_signature']
            source_code = row['source_code']
            function_method = row['function_method']

            # test executed successfully, obtain coverage data
            coverage_data = self.get_coverage_data(function_method)
            # print(coverage_data)
            
            is_coverage_success = self.is_coverage_success(coverage_data)
            if is_coverage_success:
                print(f">>>[coverage] name: {name}, is_coverage_success: {is_coverage_success}")
            else:
                print(f">>>[coverage] name: {name}, is_coverage_success: {is_coverage_success}")
            


if __name__ == "__main__":
    coverage = Coverage("hadoop")
    coverage.get_source_code()
