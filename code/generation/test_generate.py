"""Generate tests and test cases using LLM prompts."""

import json
import re
from pathlib import Path

from extract_cflow import Data_flow
from .llm_prompt import LLMPrompt
from . import prompts


class TestGenerator:
    def __init__(self):
        self.llm_call = LLMPrompt("gpt-4o")
        cflow = Path(__file__).resolve().parents[2] / "config_path.txt"
        self.extract_cflow = Data_flow(str(cflow))
        self.system_role = prompts.generate_system_role

    def generate_validate_code(self, conf_info, source_code):
        system_role = prompts.mis_conf_system_role
        ask_prompt = (
            prompts.mis_conf_constrain_prompt
            .replace("{conf_info}", conf_info)
            .replace("{source_code}", source_code)
        )
        response = self.llm_call.ask_LLM(ask_prompt, system_role=system_role)
        return self.extract_code(response)

    def generate_test_case(self, conf_info, data_flow_summary, source_code, function_method):
        ask_prompt = (
            prompts.test_case_prompt3
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
            prompts.test_case_code3
            .replace("{conf_info}", conf_info)
            .replace("{data_flow_summary}", data_flow_summary)
            .replace("{source_code}", source_code)
            .replace("{function_method}", function_method)
            .replace("{test_case}", test_case)
        )
        response = self.llm_call.ask_LLM(ask_prompt, system_role=self.system_role)
        return self.extract_code(response)

    def coverage_test_generate(self, test_code, case, data_flow_summary, function_method, source_code):
        sys_role = prompts.coverage_system_role
        ask_prompt = (
            prompts.coverage_prompt
            .replace("{test_case}", case)
            .replace("{test_code}", test_code)
            .replace("{data_flow_summary}", data_flow_summary)
            .replace("{function_method}", function_method)
            .replace("{source_code}", source_code)
        )
        response = self.llm_call.ask_LLM(ask_prompt, system_role=sys_role)
        return self.extract_code(response)

    def extract_code(self, test_method):
        pattern = r"```java\s*(.*?)\s*```"
        matches = re.findall(pattern, test_method, re.DOTALL)
        if not matches:
            new_pattern = r"```\s*(.*?)\s*```"
            matches = re.findall(new_pattern, test_method, re.DOTALL)
            if not matches:
                new_pattern = r"'''java\s*(.*?)\s*'''"
                matches = re.findall(new_pattern, test_method, re.DOTALL)
                if not matches:
                    new_pattern = r"'''\s*(.*?)\s*'''"
                    matches = re.findall(new_pattern, test_method, re.DOTALL)
                    if not matches:
                        print("No matches found.")
                        return "None"
        return matches[0]

    def extract_filename(self, test_method) -> str:
        pattern = r"public class (\w+)"
        matches = re.findall(pattern, test_method, re.DOTALL)
        return matches[0]

    def compile_error_test_generate(self, test_code, compile_information) -> str:
        ask_prompt = (
            prompts.compile_failure_prompt
            .replace("{test_code}", test_code)
            .replace("{compile_information}", compile_information)
        )
        response = self.llm_call.ask_LLM(ask_prompt, prompts.compile_failure_system_role)
        return self.extract_code(response)
