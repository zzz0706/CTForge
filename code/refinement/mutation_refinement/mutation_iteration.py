
import argparse
import json
import os
import time
import logging
import config
import pandas as pd

from LLMTest import LLMTester
from coverage.coverage import Coverage
from mutation_testing import MutationTester


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mutation_iteration_debug.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MutationIterationTester(LLMTester):


    def exectue_test(self, conf_name, test_code):
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
       

        try:
            write_path = path + "/" + file_name + ".java"
        except:
            return False, "path"
        
        is_write_success = self.write_test(test_code, write_path)
        if not is_write_success:
    
            return False, "None"
        
        execute_path = self.get_execute_path(write_path)
        if execute_path == "None":
    
            return False, "None"

        is_compile_success, compile_info = self.execute_test_path(execute_path, file_name)
        try:
            if not is_compile_success:
              
                self.delete_test(write_path)
        except:
            print("delete_test failed")
       

        return is_compile_success, compile_info

    def iterate_conf(self, conf_name: str, tests: list[str]):
      
        df = pd.read_excel(self.info_path)
        row = df[df['name'] == conf_name]
        if row.empty:
            return []
        row = row.iloc[0]
        values = row['values']
        description = row['description']
        data_flow_summary = row['data_flow_summary']
        source_code = row['source_code']
        function_method = row['function_method']


        if not isinstance(function_method, str):
           
            if pd.isna(function_method):
               
                return []
            else:
                function_method = str(function_method)
        
        conf_info = (
            "configuration name:" + conf_name +
            "\ndefault value(may be empty):" + str(values) +
            "\ndescription:" + str(description)
        )

        results = []
        for i, test in enumerate(tests):
           
            try:
      
                test_code = self.test_generate.mutation_test_generate(
                    str(test),
                    str(conf_info),
                    str(data_flow_summary),
                    str(source_code),
                    str(function_method),
                )
               
            except Exception as e:
                continue

          
            is_compile_success, compile_info = self.exectue_test(conf_name, test_code)

          
            
            item_num = 0
            is_coverage_success = False
            while True:
                
                if is_compile_success:
                    try:
                        coverage = Coverage(self.repo_name)
                        coverage_data = coverage.get_coverage_data(function_method)
                        is_coverage_success = coverage.is_coverage_success(coverage_data)
                       
                    except Exception as e:
                       
                        is_coverage_success = False
                        break

                if is_compile_success and is_coverage_success:
                    break
                if item_num > 4:
                    break
                item_num += 1
                
                if not is_compile_success:

                    new_test_code = self.test_generate.compile_error_test_generate(
                        test_code,
                        compile_info,
                    )
                elif not is_coverage_success:
       
                    new_test_code = self.test_generate.coverage_test_generate(
                        test_code,
                        "",  # no explicit test case description
                        data_flow_summary,
                        str(function_method),
                        str(source_code),
                    )
                
             
                is_compile_success, compile_info = self.exectue_test(
                    conf_name, new_test_code
                )
                test_code = new_test_code

       
            self.mvn_clean_install()
            results.append({
                "name": conf_name,
                "iterate_num": item_num,
                "is_compile_success": is_compile_success,
                "is_coverage_success": is_coverage_success,
                "test_case": "mutation_iterate",
                "coverage": "coverage_data",
            })
        
        return results, test_code


def iterate_from_mutation(repo: str, mutation_result: str, output: str | None = None):

    with open(mutation_result, "r") as f:
        data = json.load(f)
    

    tester = MutationIterationTester(repo)
    mutation_tester = MutationTester(repo)
    records = []
    
    failed_configs = [item for item in data if not item.get("is_killed")]
  

    for i, item in enumerate(failed_configs):
        conf_name = item.get("conf_name")
        
        
        if not item.get("is_killed"):
  
            test_codes = []
            test_paths = mutation_tester._gather_tests(conf_name)
           
            
            for path in test_paths:
                try:
                    with open(path, "r") as f:
                        test_codes.append(f.read())
                  
                except OSError as e:
                   
                    pass
            
            if not test_codes:
                
                continue
            
           
            try:
                test_result, test_code = tester.iterate_conf(conf_name, test_codes)
            except Exception as e:
                continue
            
            try:
                after = mutation_tester.run_mutation_for_conf(conf_name)
                if after.get("killed", 0) > 0:
                    timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())

                    store_path = config.store_path[repo] + conf_name
                    file_name = conf_name + "_" + timestamp + ".java"
                    write_path = os.path.join(store_path, file_name)
                    if not os.path.exists(os.path.dirname(store_path)):
                        os.makedirs(os.path.dirname(store_path), exist_ok=True)
                    with open(write_path, "w") as f:
                        f.write(test_code)

            except Exception as e:
                
                continue
            
            before_data = {
                "mutations": item.get("mutations", 0),
                "killed": item.get("killed", 0),
                "coverage": item.get("coverage", 0),
                "is_killed": item.get("is_killed", False),
            }
            after_data = {
                "mutations": after.get("mutations", 0),
                "killed": after.get("killed", 0),
                "coverage": after.get("coverage", 0),
                "is_killed": after.get("is_killed", False),
            }
            
            records.append(
                {
                    "conf_name": conf_name,
                    "before": before_data,
                    "after": after_data,
                }
            )

    if output is None:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output = f"iteration_compare_{repo}_{timestamp}.json"
    
    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, "w") as f:
        json.dump(records, f, indent=4)

    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Iterate tests for configurations that fail mutation testing",
    )
    parser.add_argument("repo", help="repository name")
    parser.add_argument(
        "mutation_result",
        help="path to mutation testing result json file", 
    )
    parser.add_argument(
        "--output",
        help="where to write mutation comparison results (JSON)", 
    )
    args = parser.parse_args()
    
    try:
        iterate_from_mutation(args.repo, args.mutation_result, args.output) 
    except Exception as e:
        raise