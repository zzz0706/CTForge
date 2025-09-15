# -*- coding: utf-8 -*-

import shutil
import subprocess
import pandas as pd
import os
import re
import json
import time
from pathlib import Path

from LLMPrompt import LLMPrompt
from .path_construct import PathBuilder
from Test_Generate import TestGenerator
import config
from refinement.coverage import Coverage

model_path = "gpt-4o" 
current_dir = os.path.dirname(os.path.abspath(__file__))
# Read configuration data, generate and execute tests with other modules
class LLMTester:
    def __init__(self, reponame):
        self.repo_name = reponame
        self.execute_path = config.execute_path[self.repo_name]
        self.LLMprompt = LLMPrompt(model_path)
        self.info_path = config.info_path[self.repo_name]
        self.test_generate = TestGenerator()
        self.path_construct = PathBuilder(self.repo_name)
        self.store_path = config.store_path[self.repo_name]  # store compiled test code

        self.functional_testing()    # Config functional testing.
        self.validity_testing()   # Value validity testing
       

    def functional_testing(self):  # read dataset and execute tests
       
        df = pd.read_excel(self.info_path)  # read configuration information
        num = 0
        result = []
        for index, row in df.iterrows():

            name = row['name']
            values = row['values']
            description = row['description']
            data_flow_summary = row['summary']
            source_code = row['source_code']
            function_method = row['function_method']
            num = num + 1
           
            conf_info = "configuration name:" + name + "\ndefault value(may be empty):" + str(values) + "\ndescription:" + str(description)
            
            if name in self.get_subdirectories(config.store_path[self.repo_name]):
                print(f"conf:{name} is in the subdirectory of {config.store_path[self.repo_name]}")
                continue

            try:
                test_case = self.test_generate.generate_test_case(conf_info, data_flow_summary, source_code)
            except Exception as e:
                print(e)
                continue
            case_num = 1
            for case in test_case:  # generate test case for each workload

                case_num = case_num + 1
                try:
                    test_code = self.test_generate.generate_test_code(conf_info, data_flow_summary, source_code, str(case))
                except Exception as e:
                    print(e)
                    continue
      
                is_compile_success, compile_info = self.exectue_test(name, test_code)
                item_num = 0
                is_coverage_success = False
                while True:  

                    if is_compile_success:
                        coverage = Coverage(self.repo_name)
                        coverage_data = coverage.get_coverage_data(function_method)
                        is_coverage_success = coverage.is_coverage_success(coverage_data)

                    if is_coverage_success and is_compile_success:
                        break

                    if item_num > 4:
                        break
                    item_num = item_num + 1               
                    
                    if not is_compile_success:
                        new_test_code = self.test_generate.compile_error_test_generate(test_code, compile_info)                 
                    elif not is_coverage_success:
                        new_test_code = self.test_generate.coverage_test_generate(test_code, str(case), data_flow_summary, str(function_method), str(source_code))

                    is_compile_success, compile_info = self.exectue_test(name, new_test_code)

                    
                    test_code = new_test_code                                          

                    if compile_info == "None":                   
                        break
                    elif compile_info == "path":                     
                        break
              
              
                self.mvn_clean_install()
                
                element = {
                    "name": name,
                    "iterate_num": item_num,
                    "is_compile_success": is_compile_success,
                    "is_coverage_success": is_coverage_success,
                    "test_case": case,
                    "coverage": "coverage_data",
                }
                result.append(element)

        os.chdir(current_dir)

        # get timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        try:
            # save to result.json
            with open(f"{self.repo_name}_code_{timestamp}.json", "w") as f:
                json.dump(result, f, indent=4)
        except:
            print("result.json is not found")


    def validity_testing(self):  

        df = pd.read_excel(self.info_path)  # read configuration information
        num = 0
        result = []
        for index, row in df.iterrows():
            name = row['name']
            values = row['values']
            description = row['description']  
            source_code = row['source_code']

            num = num + 1
           
            conf_info = "configuration name:" + name + "\ndefault value(may be empty):" + str(values) + "\ndescription:" + str(description)

            if name in self.get_subdirectories(config.store_path[self.repo_name]):
                print(f"conf:{name} is in the subdirectory of {config.store_path[self.repo_name]}")
                continue

            try:
                test_code = self.test_generate.generate_validate_code(conf_info, source_code)
            except Exception as e:
                print(e)
                continue   
            
            is_compile_success, compile_info = self.exectue_test(name, test_code)
            item_num = 0
            while not is_compile_success:  
            
                if item_num > 3:
                    break
                item_num = item_num + 1               
               
                new_test_code = self.test_generate.compile_error_test_generate(test_code, compile_info)
                test_code = new_test_code
               
                is_compile_success, compile_info = self.exectue_test(name, new_test_code)
           
                if compile_info == "None":

                    continue
            element = {
                "name": name,
                "iterate_num": item_num,
                "is_compile_success": is_compile_success,
                "test_case": "case",
                "coverage": "coverage",
            }
            result.append(element)
       
        os.chdir(current_dir)
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        try:
            # save to result.json
            with open(f"{self.repo_name}_conf_{timestamp}.json", "w") as f:
                json.dump(result, f, indent=4)
        except:
            print("result.json is not found")

    def get_subdirectories(self, root_dir):
        subdirs = []
        for dirpath, dirnames, _ in os.walk(root_dir):
            for dirname in dirnames:
                subdirs.append(dirname)
        return subdirs

    def mvn_clean_install(self):
        """
            execute mvn clean install
        """
        os.chdir(self.execute_path)

        if self.repo_name == "alluxio":
            result = subprocess.run(["mvn", "clean", "install", "-DskipTests", "-Dcheckstyle.skip=true", "-Dlicense.skip=true", "-Dfindbugs.skip=true"], capture_output=True, text=True)
        else:
            result = subprocess.run(["mvn", "clean", "install", "-DskipTests"], capture_output=True, text=True)
        # whether build succeeded
        if "BUILD SUCCESS" in result.stdout:
            return True
        else:
            return False

    # simplify function_list and merge identical elements
    def simplify_function_list(self, function_list) -> list:
        # simplify function_list and merge identical elements
        function_list = function_list.split(",")
        function_list = [item.strip() for item in function_list]
        function_list = list(set(function_list))
        return function_list
    
    # remove newline and spaces
    def remove_newline_and_space(self, function_list) -> list:
        function_list = str(function_list)
        function_list = function_list.replace("\n", "")
        function_list = function_list.replace(" ", "")
        return function_list

    def split_function_lists(self, function_lists) -> list:
        # if function_lists does not end with ']', append ']]'
        if function_lists[-1] != ']':
            function_lists = function_lists + ']]'
        
        try:
            # convert function_lists to list
            function_lists = ast.literal_eval(function_lists)
        except:
            print("function_lists is not a list")
            return []
        return function_lists
    
    def spilt_workload(self,  workload):  # split workload returned by LLM
        # match [] from workload
        pattern = r'\[([\s\S]*?)\]'
        try:
            matches = re.findall(pattern, workload, re.DOTALL)
        except:
            print("workload is not a list")
            return []
        return matches

        
    def is_compile_success(self, compile_result):  # determine whether compilation succeeded
        if "COMPILATION FAILURE" in compile_result.stdout or "COMPILATION ERROR" in compile_result.stdout or "Compilation failure" in compile_result.stdout:  # check for compilation error
            return False
        return True

    def is_error_test(self, compile_result):  # determine whether there are error tests
        # use regex to extract Errors value
        match = re.search(r"Errors:\s*(\d+)", compile_result.stdout)
        if match:
            error_count = int(match.group(1))
            if error_count > 0:
                print("Errors count is greater than 0.")

                return True
            else:
                print("No errors.")
                return False
        else:
            print("Could not find error count in log.")
        return False

    def is_failure_test(self, compile_result):  # determine whether tests failed
         # use regex to extract Failure value
        match = re.search(r"Failures:\s*(\d+)", compile_result.stdout)
        if match:
            error_count = int(match.group(1))
            if error_count > 0:
                print("Failure count is greater than 0.")

                return True
            else:
                print("No Failure.")
                return False
        else:
            print("Could not find Failure count in log.")
        return False

    def get_package_name(self, test_code):  # get package name
           # regex match package name
        pattern = r"package\s+([a-zA-Z0-9_\.]+);"
        match = re.search(pattern, test_code)

        if match:
            return match.group(1)  # extract and return package name
        else:
            return "None"

    def get_path(self, test_code):  # get test write path
        # path = os.path.join(self.test_file_write_path, test_code)
        package_name = self.get_package_name(test_code)
        try:
            path = self.path_construct.build_package_path(package_name, is_test=True)
        except:
            print("path is not found")
            return "None"
        return path

    def get_file_name(self, test_code):  # get file name
        # regex match class name
        pattern = r'public class (\w+)'
        matches = re.findall(pattern, test_code, re.DOTALL)
        if matches == []:
            print("file_name is not found")
            return "None"
        return matches[0]
        
    def write_test(self, test_code, write_path) -> bool:  # write test
        try:
            with open(write_path, "w") as f:
                f.write(test_code)
            return  True
        except:
            print("write_test failed")
            return False

    def delete_test(self, write_path) -> bool:  # delete compilation error test
        try:
            os.remove(write_path)
            print(f"{write_path} is deleted")
            return True
        except:
            print("delete_test failed")
            return False

    def execute_test_path(self, test_path, file_name) -> tuple:  # execute test path and return compile info

        os.chdir(test_path)
        pwd = subprocess.run(["pwd"], capture_output=True, text=True)
        print(f"pwd:{pwd.stdout}")
        if self.repo_name == "alluxio":
            compile_result = subprocess.run(["mvn", "test", "-Dtest=" + file_name, "-DfailIfNoTests=false", "-Dcheckstyle.skip=true", "-Dlicense.skip=true", "-Dfindbugs.skip=true"], capture_output=True, text=True)
        else:
            compile_result = subprocess.run(["mvn", "test", "-Dtest=" + file_name], capture_output=True, text=True)
        is_compile_success = self.is_compile_success(compile_result)
        is_error_test = self.is_error_test(compile_result)
        is_failure_test = self.is_failure_test(compile_result)
        if not is_compile_success:
            print(file_name)
            return False, compile_result.stdout
        if is_error_test:
            print(file_name)
            return False, compile_result.stdout
        
        if is_failure_test:
            print(file_name)
            return False, compile_result.stdout
        

        return True, "Compile Success"

    def get_execute_path(self, write_path):
        p = Path(write_path)
        # If path contains 'src', build path up to it; otherwise return None
        if "src" in p.parts:
            idx = p.parts.index("src")
            parent_path = Path(*p.parts[:idx])
            return str(parent_path) + '/'
        else:
            return "None"
    
    def exectue_test(self, conf_name, test_code):  # execute test
        package_name = self.get_package_name(test_code)
        if package_name == "None":
            print("package_name is None")
            return False, "None"
        file_name = self.get_file_name(test_code)
        if file_name == "None":
            print("file_name is None")
            return False, "None"
        path = self.get_path(test_code)
        if path == "None":
            print("path is None")
            return False, "None"
        print(f"path:{path}")
        print(f"file_name:{file_name}")
        try:
            write_path = path + "/" + file_name + ".java"
        except:
            print("write_path is not found")
            return False, "path"
        
        is_write_success = self.write_test(test_code, write_path)
        if not is_write_success:
            print("write_test failed")
            return False, "None"
        
        execute_path = self.get_execute_path(write_path)
        if execute_path == "None":
            print("execute_path is None")
            return False, "None"

        is_compile_success, compile_info = self.execute_test_path(execute_path, file_name)
        try:
            if not is_compile_success:
                self.delete_test(write_path)
        except:
            print("delete_test failed")

        if is_compile_success:
            self.move_success_test(conf_name, self.store_path, write_path)

        return is_compile_success, compile_info

    def move_success_test(self, conf_name, target_path, file_path) -> bool:  # move compiled test
        # create target configuration folder if missing
        conf_path = os.path.join(target_path, conf_name)
        if not os.path.exists(conf_path):
            os.makedirs(conf_path)

        # extract file name from file_path
        file_name = os.path.basename(file_path)
        try:
            # check for duplicate file in target folder
            if os.path.exists(os.path.join(conf_path, file_name)):
                print(f"duplicate file in target folder: {file_name}")
                # rename file
                file_name = file_name.replace(".java", "_" + str(time.time()) + ".java")
                # move file
                shutil.move(file_path, os.path.join(conf_path, file_name))
                return True
            else:
                # move file
                shutil.move(file_path, os.path.join(conf_path, file_name))
                return True
        except:
            print(f"move file failed: {file_path}")
            return False



if __name__ == '__main__':
    tester = LLMTester("zookeeper")
