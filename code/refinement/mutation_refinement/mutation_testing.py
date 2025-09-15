import os
import re
import json
import csv
import subprocess
import time
import shutil

from runCtestByConf.runLLM4Ctest import RunnerTest, generated_test_path, execute_path
import config

current_path = os.path.dirname(os.path.abspath(__file__))

class MutationTester:

    def __init__(self, repo_name: str):
        self.repo_name = repo_name
        self.runner = RunnerTest(repo_name)
        self.test_paths = generated_test_path[repo_name]
        self.execute_path = execute_path[repo_name]

    def _gather_tests(self, conf_name: str):
        paths = []
        for root in self.test_paths:
            paths.extend(self.runner.find_files_in_matching_folders(root, conf_name))
        return paths

    def _write_tests(self, conf_paths):

        test_classes = [] 
        written_paths = []
        packages = set()

        for path in conf_paths:
            with open(path, "r") as f:
                content = f.read()

            write_path, file_name = self.runner.get_write_path(content)
            if write_path == "None":
                continue

            package = self.runner.get_package_name(content)
            if package == "None":
                package = ""

            unique_name = file_name
            unique_content = content
            unique_path = write_path
            idx = 1
            while os.path.exists(unique_path) or (package and f"{package}.{unique_name}" in test_classes):
                unique_name = f"{file_name}_{idx}"
                unique_path = write_path.replace(file_name + ".java", unique_name + ".java")
                unique_content = re.sub(rf"(public\s+class\s+){re.escape(file_name)}(\b)", rf"\1{unique_name}\2", content)
                idx += 1

            if self.runner.write_test(unique_content, unique_path):
                fqcn = f"{package}.{unique_name}" if package else unique_name
                test_classes.append(fqcn)
                written_paths.append(unique_path)
                if package:
                    packages.add(package)

        return test_classes, written_paths, list(packages)

    def _copy_tests_to_store_path(self, conf_name: str, written_paths: list):

        if not hasattr(config, 'store_path') or self.repo_name not in config.store_path:
            print(f"No store path configured for {self.repo_name}")
            return
            
        store_path = config.store_path[self.repo_name]
        conf_path = os.path.join(store_path, conf_name)
        
     
        if not os.path.exists(conf_path):
            os.makedirs(conf_path)
        
        for src_path in written_paths:
            if os.path.exists(src_path):
                file_name = os.path.basename(src_path)
                dest_path = os.path.join(conf_path, file_name)
                
                if os.path.exists(dest_path):

                    name, ext = os.path.splitext(file_name)
                    file_name = f"{name}_{int(time.time())}{ext}"
                    dest_path = os.path.join(conf_path, file_name)
                
                
                shutil.copy2(src_path, dest_path)
    

    def _cleanup_tests(self, written_paths):
        for wp in written_paths:
            self.runner.delete_test(wp)

    def _parse_pitest_output(self, output: str):
        total = killed = 0
        coverage = 0.0

        summary_match = re.search(
            r">>\s*Generated\s+(\d+)\s+mutations\s+Killed\s+(\d+)(?:\s+\(([\d.]+)%\))?",
            output,
            flags=re.IGNORECASE
        )
        
        if summary_match:
            total, killed = int(summary_match.group(1)), int(summary_match.group(2))
            print(f"Found summary: total={total}, killed={killed}")
        else:
           
            pattern1 = re.search(r"Generated\s+(\d+)\s+mutations\.\s+Killed\s+(\d+)\.", output, flags=re.IGNORECASE)
            if pattern1:
                total, killed = int(pattern1.group(1)), int(pattern1.group(2))
                print(f"Found pattern1: total={total}, killed={killed}")
            
            if total == 0 and killed == 0:
                pattern2 = re.search(r"Generated\s+(\d+)\s+mutations\s+Killed\s+(\d+)", output, flags=re.IGNORECASE)
                if pattern2:
                    total, killed = int(pattern2.group(1)), int(pattern2.group(2))
                    print(f"Found pattern2: total={total}, killed={killed}")

        if total:
            coverage = killed / total * 100
        else:
            print("Could not parse mutation results from PIT output")
            
        result = {
            "mutations": total,
            "killed": killed,
            "coverage": coverage,
            "is_killed": killed > 0
        }
        
        print(f"Parsed result: {result}")
        return result

    def run_mutation_for_conf(self, conf_name: str):
        conf_paths = self._gather_tests(conf_name)
        if not conf_paths:
            print(f"No tests found for {conf_name}")
            return {"conf_name": conf_name, "mutations": 0, "killed": 0, "coverage": 0, "is_killed": False}

        test_classes, written_paths, packages = self._write_tests(conf_paths)
        if not test_classes:
            print(f"No test classes written for {conf_name}")
            return {"conf_name": conf_name, "mutations": 0, "killed": 0, "coverage": 0, "is_killed": False}

        os.chdir(self.execute_path)
        cmd = [
            "mvn",
            "org.pitest:pitest-maven:mutationCoverage",
            "-Djacoco.skip=true",
            "-DjacocoArgLine=\"\""
        ]
        if packages:
            targets = ','.join(p + '.*' for p in packages)
            cmd.append(f"-DtargetClasses={targets}")
        for t in test_classes:
            cmd.append(f"-DtargetTests={t}")
        cmd.append("-DfailWhenNoMutations=false")
        cmd.append("-DargLine=\"\"")
        
        print(f"Running PIT mutation testing: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(f"PIT mutation testing completed with return code {result.returncode}")
        
        # Print output for debugging
        if result.stdout:
            print(f"STDOUT: {result.stdout}")
        if result.stderr:
            print(f"STDERR: {result.stderr}")
        os.chdir(current_path)
        self._cleanup_tests(written_paths)

        # Parse PIT output to extract mutation testing results
        info = self._parse_pitest_output(result.stdout)
        info.update({"conf_name": conf_name})
        
        # Add additional information for debugging
        info.update({
            "return_code": result.returncode,
            "stdout_length": len(result.stdout),
            "stderr_length": len(result.stderr)
        })
        
        return info

    def run_all(self):
        data = []
        confs = set()
        for root in self.test_paths:
            confs.update(os.listdir(root))
        for conf_name in sorted(confs):
            if conf_name.startswith('.'):
                continue
            print(f"Running mutation tests for {conf_name}")
            data.append(self.run_mutation_for_conf(conf_name))
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        base = f"result/mutation/mutation_{self.repo_name}_{timestamp}"
        json_path = base + ".json"
        csv_path = base + ".csv"
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        with open(json_path, "w") as f:
            json.dump(data, f, indent=4)

        with open(csv_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=["conf_name", "mutations", "killed", "coverage", "is_killed"],
                extrasaction='ignore',
            )
            writer.writeheader()
            for item in data:
                writer.writerow(item)

        return data


if __name__ == "__main__":
    import argparse
    import time
    parser = argparse.ArgumentParser(description="Run mutation testing on generated tests")
    parser.add_argument("repo", help="repository name")
    parser.add_argument("conf", nargs='?', help="configuration name")
    args = parser.parse_args()
    tester = MutationTester(args.repo)
    if args.conf:
        result = tester.run_mutation_for_conf(args.conf)
        print(json.dumps(result, indent=4))
    else:
        tester.run_all()
        print("All mutation tests completed.")