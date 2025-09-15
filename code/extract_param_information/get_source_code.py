import re
import os
import sys
import json
import javalang
from path_construct import Path_construct
from code_instrumentation import CodeInstrumentation

repo_path = {"hadoop": "hadoop/hadoop-common-project/hadoop-common/",
             "hdfs": "hadoop/hadoop-hdfs-project/",
             "hbase": "hbase/"}



class SourceCode:
    def __init__(self):
        self.path = repo_path["hbase"]
        self.code = ""
        self.path_construct = Path_construct("hbase")
        self.code_instrumentation = CodeInstrumentation()
        #self.read_code()

    def get_source_code_from_functionlist(self, function_list):

        function_list = self.parser_function_list(function_list)
        result = []
        for i in function_list:
            str = json.loads(i)
            for j in str:    
              
                java_code = self.parser_single_function_from_function_list(j["class_name"])
                function_name = j["function_name"]
                if self.filter_function_list(j["class_name"], j["function_name"]):
                    continue
                else:
                    result.append(self.parser_source_code(java_code, function_name))
             
        result = list(set(result))
        return result

    
    def parser_function_list(self, function_list):
      
        try:
            pattern = r'```json\s*(.*?)\s*```'
            matches = re.findall(pattern, function_list, re.DOTALL)          
           
        except:
                      return None
        return matches

    def parser_single_function_from_function_list(self, class_name):
     
        last_dot_index = class_name.rfind(".")

        if last_dot_index != -1:
            package_name = class_name[:last_dot_index]
        else:
            result = class_name  

        file_path = self.path_construct.build_package_path(package_name, is_test=False)
        class_name = class_name.split(".")[-1]
       
        if "$" in class_name:
            file_name = class_name.split("$")[0]
            class_name = class_name.split("$")[1]
            file_path = os.path.join(file_path, file_name + ".java")
        else:
            file_path = os.path.join(file_path, class_name + ".java")

        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        return content


    def parser_source_code(self, java_code, function_name):
        
        tree = javalang.parse.parse(java_code)
    
        function_name = function_name.split("(")[0]

        target_function_name = function_name
        target_function_code = None

        for _, node in tree:
            if isinstance(node, javalang.tree.MethodDeclaration) and node.name == target_function_name:
               
                start_line = node.position[0] - 1  
                end_line = node.position[1] - 1

                java_lines = java_code.splitlines()

                method_end_line = None
                brace_count = 0
                for idx in range(start_line, len(java_lines)):
                    line = java_lines[idx]
                    brace_count += line.count("{") - line.count("}")
                    if brace_count == 0:
                        method_end_line = idx
                        break

                if method_end_line is not None:
                    function_code = "\n".join(java_lines[start_line:method_end_line+1])
                    target_function_code = function_code
                    break


        if target_function_code:
            return target_function_code
        else:
            return None

    def filter_function_list(self, class_name, function_name):

        if "Configuration" in class_name or "org.apache.hadoop" not in class_name:
            return True
        else:
            return False
            

    def find_method_in_java_files(self, package_name, class_name, method_name):

        project_dir = self.path_construct.build_package_path(package_name)
        method_pattern = re.compile(r'\b' + re.escape(method_name) + r'\b')
        method_definition_pattern = re.compile(r'(\s*public\s+.*\s+' + re.escape(method_name) + r'\s*\(.*\)\s*\{)')
        method_body_pattern = re.compile(r'\s*\{(.*?)\}', re.DOTALL)  
        
        result = []

        for root, dirs, files in os.walk(project_dir):
            for file in files:
                if file.endswith(".java"):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()

            
                            if method_pattern.search(content):
                                matches = method_definition_pattern.findall(content)
                                for match in matches:
                                    method_name_found = match.strip()
                                    print(f"Found method: {method_name_found} in {file_path}")

                                    body_match = method_body_pattern.search(content)
                                    if body_match:
                                        method_body = body_match.group(1).strip()
                                        result.append({
                                            'method_name': method_name_found,
                                            'method_body': method_body,
                                            'file_path': file_path
                                        })

                    except Exception as e:
                        print(f"Error reading file {file_path}: {e}")

        return result
    
    def get_source_code(self, function_name):
        path = self.path_construct.get_path(function_name)
        return self.find_method_in_java_files(path, function_name)
