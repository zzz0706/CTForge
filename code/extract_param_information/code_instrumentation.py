import os
import re
import javalang
import json
import random
import sys
import pandas as pd
#from get_source_code import SourceCode
from path_construct import Path_construct

class CodeInstrumentation:
    
    def __init__(self) -> None:
        self.path_construct = Path_construct("hadoop")
 
        self.read_path = "hadoop_common.xlsx"
        self.repo_path = "hadoop-common-project/hadoop-common/"   
  
        self.store_path = ""
        
  

    def parser_function_list(self, function_list):

        try:
            pattern = r'```json\s*(.*?)\s*```'
            matches = re.findall(pattern, function_list, re.DOTALL)          
            # function_list = json.loads(str)
        except:

            return None
        return matches

    def collect_function_list(self, file_path, store_path):
      
        df = pd.read_excel(file_path)
        data_insert = []
        for index, row in df.iterrows():
          
            function_list = row['function_method']
            
           
            function_list = self.parser_function_list(function_list)
            for function_name in function_list: 
                
                functions = json.loads(function_name)
                for function in functions:
                    if function in data_insert:
                        continue
                    data_insert.append(function)

                    class_name = function['class_name']
                    function_name = function['function_name']
       
            

        print(data_insert)

        with open(store_path, "w", encoding="utf-8") as f:
            json.dump(data_insert, f, ensure_ascii=False)

    def parser_single_function_to_path(self, class_name):
        
  
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

        return file_path

    def get_function_name(self, function_name):
      
        name = function_name.split("(")[0]
        return name
    
    def get_init_function_name(self, class_name, function_name):
       
        if "$" in class_name:
            class_name = class_name.split("$")[1]
            return class_name
        class_name = class_name.split(".")[-1]
        return class_name

    def get_java_name(self, file_path):
  
        name = file_path.split("/")[-1]
        return name

    def is_init(self, function_name):
        
        if "<init>" in function_name:
            return True
        else:
            return False


    def read_function_list(self, file_path, repo_path):
        
        with open(file_path, "r", encoding="utf-8") as f:
            data_insert = json.load(f)
        num = 0
        for function in data_insert:
          
            function_name = function['function_name']
            class_name = function['class_name']
           
            file_path = self.parser_single_function_to_path(class_name)


            if self.is_init(function_name):
                function_name = self.get_init_function_name(class_name, function_name)
            else:
                function_name = self.get_function_name(function_name)

            self.instrument_java_file(file_path, function_name)
            num += 1

    def get_insert_str(self, function_name: str) -> str:

        str = f"LOG.warn (\"[ CTEST ][{function_name}] \"); "
        return str
        

    def instrument_code(self, file_path: str, function_name: str) -> str:
       

        insert_str = self.get_insert_str(function_name)

        java_file_path = file_path 
        with open(java_file_path, "r", encoding="utf-8") as f:
            java_code = f.read()

        lines = java_code.splitlines()

        try:
            tree = javalang.parse.parse(java_code)
        except javalang.parser.JavaSyntaxError as e:
            exit()

     
        target_method_name = function_name
        method_line = None 

        for path, node in tree:
            if isinstance(node, javalang.tree.MethodDeclaration):
                if node.name == target_method_name:
                    if node.position:
                        method_line = node.position.line
                        break

        if method_line is None:
            return False

     
        insert_line_index = None
        for i in range(method_line - 1, len(lines)):
            if "{" in lines[i]:
               
                insert_line_index = i + 1
                break

        if insert_line_index is None:
            return False

        
        instrumentation_code = f" \tLOG.warn (\"[ LLM4CTEST ][{function_name}] \");"
        lines.insert(insert_line_index, instrumentation_code)
        
        modified_code = "\n".join(lines)

        
        random_number = random.randint(1, 1000000)


        output_file_path = f"TargetFile_instrumented_{random_number}.java"
        with open(output_file_path, "w", encoding="utf-8") as f:
            f.write(modified_code)
        
        return True
        
    def instrument_java_file(self, java_file_path, target_function_name):
        
        with open(java_file_path, "r", encoding="utf-8") as f:
            java_code = f.read()
        

        lines = java_code.splitlines()
        

        try:
            tree = javalang.parse.parse(java_code)
        except javalang.parser.JavaSyntaxError as e:
            return     
      
        insert_points = []
        
        for path, node in tree:

            if isinstance(node, javalang.tree.MethodDeclaration):
                if node.name == target_function_name:
                    if node.position:
                        method_line = node.position.line
                       
                        for i in range(method_line - 1, len(lines)):
                            if "{" in lines[i]:
                                insert_line_index = i + 1
                           
                                instrumentation_code = f"\tLOG.warn(\"[ LLM4CTEST ][{target_function_name}]\");"
                                insert_points.append((insert_line_index, instrumentation_code))
                                break
          
            elif isinstance(node, javalang.tree.ConstructorDeclaration):
                if node.name == target_function_name:
                    if node.position:
                        constructor_line = node.position.line
                        for i in range(constructor_line - 1, len(lines)):
                            if "{" in lines[i]:
                                insert_line_index = i + 1
                                instrumentation_code = f"\tLOG.warn(\"[ LLM4CTEST ][{target_function_name}]\");"
                                insert_points.append((insert_line_index, instrumentation_code))
                                break
        
        if not insert_points:
            return
        
        for index, code in sorted(insert_points, key=lambda x: x[0], reverse=True):
            lines.insert(index, code)
        
        modified_code = "\n".join(lines)
        random_number = random.randint(1, 1000000)
        output_file_path = f"_instrumented_{random_number}.java"
        with open(output_file_path, "w", encoding="utf-8") as f:
            f.write(modified_code)


    