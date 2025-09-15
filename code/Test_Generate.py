from LLMPrompt import LLMPrompt
import re
import ast
import os
from extract_param_information.extract_cflow import Data_flow
import prompt  
import json
import config

class Test_Generate:
    def __init__(self):
        self.llmCall = LLMPrompt(config.LLM_model)
        # self.test_generate_prompt = constant.test_generate_prompt
        self.cflow_path = "hadoop_common.txt"
        self.extract_cflow = Data_flow(self.cflow_path)
        self.system_role = prompt.generate_system_role

   
    def generate_validate_code(self, conf_info, source_code):
        system_role = prompt.mis_conf_system_role
        ask_prompt = (
            prompt.mis_conf_constrain_prompt
            .replace("{conf_info}", conf_info)
            .replace("{source_code}", source_code)
        )
        response = self.llm_call.ask_LLM(ask_prompt, system_role=system_role)
        return self.extract_code(response)

    def mutation_test_generate(self, test_code, conf_info, data_flow_summary, source_code, function_method):
        sys_role = prompt.mutation_system_role
        ask_prompt = (
            prompt.mutation_prompt
            .replace("{test_code}", test_code)
            .replace("{conf_info}", conf_info)
            .replace("{data_flow_summary}", data_flow_summary)
            .replace("{source_code}", source_code)
            .replace("{function_method}", function_method)
        )
        response = self.llmCall.ask_LLM(ask_prompt, system_role=sys_role)
        return self.extract_code(response)
    
    def generate_test_case(self, conf_info, data_flow_summary, source_code, function_method):
        ask_prompt = (
            prompt.test_case_prompt3
            .replace("{conf_info}", conf_info)
            .replace("{data_flow_summary}", data_flow_summary)
            .replace("{source_code}", source_code)
            .replace("{function_method}", function_method)
        )
        response = self.llm_call.ask_LLM(ask_prompt, system_role=self.system_role)
        pattern = r"```json\s*(.*?)\s*```"
        matches = re.findall(pattern, response, re.DOTALL)
        try:
            return json.loads(matches[0])
        except Exception:  # noqa: BLE001
            print("No test cases found.")
            return []

    def generate_test_code(self, conf_info, data_flow_summary, source_code, function_method, test_case):
        ask_prompt = (
            prompt.test_case_code3
            .replace("{conf_info}", conf_info)
            .replace("{data_flow_summary}", data_flow_summary)
            .replace("{source_code}", source_code)
            .replace("{function_method}", function_method)
            .replace("{test_case}", test_case)
        )
        response = self.llm_call.ask_LLM(ask_prompt, system_role=self.system_role)
        return self.extract_code(response)

    def coverage_test_generate(self, test_code, case, data_flow_summary, function_method, source_code):
        sys_role = prompt.coverage_system_role
        ask_prompt = prompt.coverage_prompt.replace("{test_case}", case) \
            .replace("{test_code}", test_code) \
            .replace("{data_flow_summary}", data_flow_summary) \
            .replace("{function_method}", function_method) \
            .replace("{source_code}", source_code)
        response = self.llmCall.ask_LLM(ask_prompt, system_role=sys_role)
        return self.extract_code(response)

    def extract_code(self, test_method):  
        
        pattern = r'```java\s*(.*?)\s*```'
        matches = re.findall(pattern, test_method, re.DOTALL)
        if matches == []:
            new_pattern = r'```\s*(.*?)\s*```' 
            new_matches = re.findall(new_pattern, test_method, re.DOTALL)
            if new_matches == []:
                new_pattern = r"'''java\s*(.*?)\s*'''"
                new_matches = re.findall(new_pattern, test_method, re.DOTALL)
                if new_matches == []:
                    new_pattern = r"'''\s*(.*?)\s*'''"
                    new_matches = re.findall(new_pattern, test_method, re.DOTALL)
                    if new_matches == []:
                            print("No matches found.")
                            return "None"
                    return new_matches[0]
                return new_matches[0]
            # print(new_matches)
            return new_matches[0]
        # print(f"{matches}")
        return matches[0]
    
    def extract_filename(self, test_method) -> str:
        pattern = r'public class (\w+)'
        matches = re.findall(pattern, test_method, re.DOTALL)
        return matches[0]
    
    def compile_error_test_generate(self, test_code, compile_information) -> tuple: 
        ask_prompt = prompt.compile_failure_prompt.replace("{test_code}", test_code) \
            .replace("{compile_information}", compile_information)  
        response = self.llmCall.ask_LLM(ask_prompt, prompt.compile_failure_system_role)
        response_code = self.extract_code(response)
        return response_code
    

