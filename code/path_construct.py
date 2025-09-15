
import os
import re
import ast
import json
from pathlib import Path
import config


class Path_construct:
    def __init__(self, repo_name):
        self.repo_name = repo_name

        self.repo_path = config.repo_path[repo_name]

    def find_java_package(self, project_dir): 
        packages = set()
        for root, dirs, files in os.walk(project_dir):
            for file in files:
                if file.endswith(".java"):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        match = re.search(r'^\s*package\s+([a-zA-Z0-9._]+);', content, re.MULTILINE)
                        if match:
                            package = match.group(1)
                            packages.add(package)
        return sorted(packages)

    def get_subdirectories(self, directory):
        p = Path(directory)
        return [str(child) for child in p.iterdir() if child.is_dir()]

    def find_java_packages_with_absolute_paths(self, project_root):
        package_paths = {}

        for root, dirs, files in os.walk(project_root):
            for file in files:
                if file.endswith(".java"):
                    file_path = os.path.join(root, file)

                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
            
                        match = re.search(r'^\s*package\s+([a-zA-Z0-9._]+);', content, re.MULTILINE)
                        if match:
                        
                            package = match.group(1)

                            package_dir = package.replace('.', os.sep)

                            absolute_path = os.path.join(project_root, package_dir)
                            
                            if package not in package_paths:
                                package_paths[package] = absolute_path
        return package_paths
    
    def get_all_subdirectories_absolute_paths(self, root_dir):
  
        subdirectories = []
        try:
      
            for root, dirs, files in os.walk(root_dir):
                for dir_name in dirs:
                   
                    subdirectory_path = os.path.join(root, dir_name)
                    subdirectories.append(subdirectory_path)

        except Exception as e:
            print(f"Error: {e}")
            root_dir = config.repo_path[self.repo_name]
            for root, dirs, files in os.walk(root_dir):
                for dir_name in dirs:
                    subdirectory_path = os.path.join(root, dir_name)
                    subdirectories.append(subdirectory_path)
        return subdirectories

    def build_package_test_path(self, package_name): 
        package_path = package_name.replace(".", "/")
        all_path = self.get_all_subdirectories_absolute_paths(self.repo_path)
        for path in all_path:
            if package_path in path:
                return path
        return None

    def build_package_path(self, package_name, is_test=True):
       
        if not package_name or package_name == "None":
            return None
            
        package_path = package_name.replace(".", "/")
       
        if not is_test:
            str = "/main/"
        else:
            str = "/test/"
        
        for item in self.repo_path:
            all_path = self.get_all_subdirectories_absolute_paths(item)
    
            for path in all_path:
               
                path_folder = os.path.basename(path)
               
                package_folder = os.path.basename(package_path)
               
                if package_path in path and path_folder == package_folder and str in path:
                    return path
                else:
                    continue
       
        return None
        

    def get_test_write_path(self, package_name): 
        return package_name

if __name__ == "__main__":
 
    print(Path_construct("zookeeper").build_package_path("org.apache.zookeeper.test", is_test=True))
