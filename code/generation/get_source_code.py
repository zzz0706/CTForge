import os
import json
import re
import javalang

from .path_construct import PathBuilder
from .. import config


class SourceCode:
    def __init__(self, repo_name: str = "hbase"):
        self.repo_name = repo_name
        self.path = config.repo_path[repo_name][0]
        self.code = ""
        self.path_builder = PathBuilder(repo_name)

    def get_source_code_from_functionlist(self, function_list):
        """Retrieve source code for each function in ``function_list``."""
        function_list = self.parser_function_list(function_list)
        result = []
        for item in function_list:
            obj = json.loads(item)
            for j in obj:
                java_code = self.parser_single_function_from_function_list(j["class_name"])
                function_name = j["function_name"]
                if self.filter_function_list(j["class_name"], j["function_name"]):
                    continue
                result.append(self.parser_source_code(java_code, function_name))
        return list(set(result))

    def parser_function_list(self, function_list):
        """Parse function list from Excel into JSON strings."""
        pattern = r"```json\s*(.*?)\s*```"
        try:
            return re.findall(pattern, function_list, re.DOTALL)
        except Exception:  # noqa: BLE001
            print("parse function list failed")
            return []

    def parser_single_function_from_function_list(self, class_name):
        """Return Java source code for a class name."""
        last_dot_index = class_name.rfind(".")
        package_name = class_name[:last_dot_index] if last_dot_index != -1 else class_name
        file_path = self.path_builder.build_package_path(package_name, is_test=False)
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
        """Extract source code for ``function_name`` from ``java_code``."""
        tree = javalang.parse.parse(java_code)
        function_name = function_name.split("(")[0]
        target_function_code = None
        for _, node in tree:
            if isinstance(node, javalang.tree.MethodDeclaration) and node.name == function_name:
                start_line = node.position[0] - 1
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
                    function_code = "\n".join(java_lines[start_line:method_end_line + 1])
                    target_function_code = function_code
                    break
        if target_function_code:
            return target_function_code
        print(f"Could not find source code for {function_name}")
        return None

    def filter_function_list(self, class_name, function_name):
        """Return True for functions that should be filtered out."""
        return "Configuration" in class_name or "org.apache.hadoop" not in class_name

    def find_method_in_java_files(self, package_name, class_name, method_name):
        """Search project for source code by package and method names using regex."""
        project_dir = self.path_builder.build_package_path(package_name)
        method_pattern = re.compile(r"\b" + re.escape(method_name) + r"\b")
        method_definition_pattern = re.compile(
            r'(\s*public\s+.*\s+' + re.escape(method_name) + r'\s*\(.*\)\s*\{)'
        )
        method_body_pattern = re.compile(r"\s*\{(.*?)\}", re.DOTALL)
        result = []
        for root, _, files in os.walk(project_dir):
            for file in files:
                if file.endswith(".java"):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            content = f.read()
                        if method_pattern.search(content):
                            matches = method_definition_pattern.findall(content)
                            for match in matches:
                                body_match = method_body_pattern.search(content)
                                if body_match:
                                    result.append({
                                        "method_name": match.strip(),
                                        "method_body": body_match.group(1).strip(),
                                        "file_path": file_path,
                                    })
                    except Exception as e:  # noqa: BLE001
                        print(f"Error reading file {file_path}: {e}")
        return result
