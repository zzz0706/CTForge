"""Utility helpers for building Java package and test paths."""

import os
import re
from pathlib import Path

from .. import config


class PathBuilder:
    """Construct file system paths based on Java package names."""

    def __init__(self, repo_name: str):
        self.repo_name = repo_name
        self.repo_path = config.repo_path[repo_name]

    def find_java_package(self, project_dir: str):
        """Return all package names under a project directory."""
        packages = set()
        for root, _, files in os.walk(project_dir):
            for file in files:
                if file.endswith(".java"):
                    file_path = os.path.join(root, file)
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                    match = re.search(r"^\s*package\s+([a-zA-Z0-9._]+);", content, re.MULTILINE)
                    if match:
                        packages.add(match.group(1))
        return sorted(packages)

    def get_subdirectories(self, directory: str):
        p = Path(directory)
        return [str(child) for child in p.iterdir() if child.is_dir()]

    def find_java_packages_with_absolute_paths(self, project_root: str):
        """Map package names to absolute paths."""
        package_paths = {}
        for root, _, files in os.walk(project_root):
            for file in files:
                if file.endswith(".java"):
                    file_path = os.path.join(root, file)
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                    match = re.search(r"^\s*package\s+([a-zA-Z0-9._]+);", content, re.MULTILINE)
                    if match:
                        package = match.group(1)
                        package_dir = package.replace(".", os.sep)
                        absolute_path = os.path.join(project_root, package_dir)
                        package_paths.setdefault(package, absolute_path)
        return package_paths

    def get_all_subdirectories_absolute_paths(self, root_dir):
        """Return all subdirectory paths under ``root_dir``."""
        subdirectories = []
        try:
            for root, dirs, _ in os.walk(root_dir):
                for dir_name in dirs:
                    subdirectory_path = os.path.join(root, dir_name)
                    subdirectories.append(subdirectory_path)
        except Exception as e:  # noqa: BLE001
            print(f"Error: {e}")
            root_dir = config.repo_path[self.repo_name]
            for root, dirs, _ in os.walk(root_dir):
                for dir_name in dirs:
                    subdirectory_path = os.path.join(root, dir_name)
                    subdirectories.append(subdirectory_path)
        return subdirectories

    def build_package_test_path(self, package_name: str):
        """Return path containing tests for ``package_name``."""
        package_path = package_name.replace(".", "/")
        for path in self.get_all_subdirectories_absolute_paths(self.repo_path):
            if package_path in path:
                return path
        return None

    def build_package_path(self, package_name: str, is_test: bool = True):
        """Construct file path for ``package_name`` under source or test directories."""
        package_path = package_name.replace(".", "/")
        segment = "/test/" if is_test else "/main/"
        for item in self.repo_path:
            for path in self.get_all_subdirectories_absolute_paths(item):
                path_folder = os.path.basename(path)
                package_folder = os.path.basename(package_path)
                if package_path in path and path_folder == package_folder and segment in path:
                    return path
        return None

    def get_test_write_path(self, package_name: str):
        """Placeholder for constructing write path for generated tests."""
        return package_name


if __name__ == "__main__":
    pass