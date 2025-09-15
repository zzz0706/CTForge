import os
import subprocess
import shutil
import json
import re
import pandas as pd
import time
import sys
from path_construct import Path_construct
from runCtest import  is_compile_failed, is_error_test, is_failure_test, is_run_test


generated_test_path = {
    "hadoop": [
        "hadoop/kimi/code/",
               ],
    "hdfs"  : ["hdfs/kimi/code/",]
}

execute_path = {
    "hadoop" : "hadoop/hadoop-common-project/hadoop-common/",
}

current_path = os.path.dirname(os.path.abspath(__file__))


class RunnerTest:
    def __init__(self, repo_name):
        # self.test_path = test_path
        self.repo_name = repo_name
        self.path_construct = Path_construct(self.repo_name)
    
    
    def run_test(self, file_name):

        os.chdir(execute_path[self.repo_name])

        if self.repo_name == "alluxio":
            # file_name = "test.class"
            cmd = ["mvn", "test", "-Dtest=" + file_name, "-DfailIfNoTests=false", "-Dcheckstyle.skip=true", "-Dlicense.skip=true", "-Dfindbugs.skip=true"]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  
            except subprocess.TimeoutExpired:

                class Result:
                    pass
                result = Result()
                result.returncode = 1
                result.stdout = ""
                result.stderr = "Timeout: Command exceeded 10 minutes"
                result.args = cmd
            return result
        else:
            cmd = ["mvn", "test", "-Dtest=" + file_name]
            print(f">>>[run_test]\tcmd: {cmd}")
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  
            except subprocess.TimeoutExpired:

                class Result:
                    pass
                result = Result()
                result.returncode = 1
                result.stdout = ""
                result.stderr = "Timeout: Command exceeded 10 minutes"
                result.args = cmd
            return result



    def get_file_name(self, test_code): 

        pattern = r'public class (\w+)'
        matches = re.findall(pattern, test_code, re.DOTALL)
        if matches == []:
            print("file_name is not found")
            return "None"
        return matches[0]

    def get_package_name(self, test_code): 
        pattern = r"package\s+([a-zA-Z0-9_\.]+);"
        match = re.search(pattern, test_code)
        
        if match:
            return match.group(1)  
        else:
            return "None"

    def get_write_path(self, content):
        
        file_name = self.get_file_name(content)
        class_name = self.get_package_name(content)


        if self.path_construct.build_package_path(class_name) == "None" or self.path_construct.build_package_path(class_name) is None:
            return "None", "None"

        path = self.path_construct.build_package_path(class_name)

        if not os.path.exists(path):
            print(f">>>[get_write_path]\tpath: {path} is not found")
            return "None", "None"

        write_path = str(path) + "/" + file_name + ".java"
        return write_path, file_name
        

    def find_files_in_matching_folders(self, root_folder, match_str):
      
        matched_files = []
        found_match = False  

        for item in os.listdir(root_folder):
            sub_folder_path = os.path.join(root_folder, item)

            if os.path.isdir(sub_folder_path):

                if match_str in item:
                    found_match = True

                    for dirpath, _, filenames in os.walk(sub_folder_path):
                        for filename in filenames:
                            file_full_path = os.path.join(dirpath, filename)
                            matched_files.append(file_full_path)
        
        if not found_match:
            return []
        
        return matched_files
    
    def delete_test(self, write_path) -> bool:
        if os.path.exists(write_path):
            os.remove(write_path)
            return True
        else:
            return False
        

        
    def write_test(self, test_code, write_path) -> bool: 
        try:
            with open(write_path, "w") as f:
                f.write(test_code)
            if os.path.exists(write_path):
                return True
            else:
                print("write_test failed")
                return False
        except:
            print("write_test failed")
            return False



    def runTestByConf(self, conf_name):
      
        conf_paths = []
        for path in generated_test_path[self.repo_name]:
            conf_paths.extend(self.find_files_in_matching_folders(path, conf_name))

        data = []
        is_compile_num = 0
        is_error_num = 0
        is_failure_num = 0
        is_run_num = 0

        for path in conf_paths:   
           
            with open(path, 'r') as file:   
                content = file.read()
            try:
                write_path, file_name = self.get_write_path(content)
            except:
                continue

           
            if write_path == "None":
                print(f">>>[runTestByConf]\twrite_path is not found")
                continue

            if self.write_test(content, write_path):
                result = self.run_test(file_name)
               
                if self.delete_test(write_path):     
                    pass
               
              
                if is_compile_failed(result):
                    is_compile_num += 1
                    info = "compile failed"
                    
                elif is_error_test(result):
                    is_error_num += 1
                    info = "error test"
                 
                elif is_failure_test(result):
                    is_failure_num += 1
                    info = "failure test"
                
                elif not is_run_test(result):
                    is_run_num += 1
                    info = "no run test"
                else:
                    info = "success"
                       
            else:
                
                continue
            
         


            element = {
                "conf_name": conf_name,
                "test_name": path,
                "write_path": write_path,
                "test_info": info,
            }
            data.append(element)
            if info == "error test" or info == "failure test":
                print(f">>>>conf: {conf_name}, test: {path}, info: {info}")
                # break

        os.chdir(current_path)


        timestamp = time.strftime('%Y%m%d_%H%M%S')
        store_path = f"result/singleConf/LLM4Ctest_{self.repo_name}_{conf_name}_{timestamp}.json"
        with open(store_path, "w") as f:
            json.dump(data, f, indent=4)

        return data
    
    def runAllTest(self):
      
        data = []
        for conf_name in os.listdir(generated_test_path[self.repo_name][0]):
            
            if conf_name == "clientPort":
                continue
            data.append(self.runTestByConf(conf_name)) 
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        store_path = f"{self.repo_name}_all_{timestamp}.json"
        with open(store_path, "w") as f:
            json.dump(data, f, indent=4)    
        
        return data
             

if __name__ == "__main__":
    repo_name = "zookeeper"
    runner = RunnerTest(repo_name)
    runner.runAllTest()

    
