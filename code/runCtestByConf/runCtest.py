import os
import json
import re
import subprocess
from tqdm import tqdm
import time
import sys
sys.path.append('..')
import config
import pandas as pd


def is_compile_failed(compile_result): 
        if "COMPILATION FAILURE" in compile_result.stdout or "COMPILATION ERROR" in compile_result.stdout or "Compilation failure" in compile_result.stdout: # 准备修改，是否有编译错误
            return True
        return False

def is_error_test( compile_result): 
    match = re.search(r"Errors:\s*(\d+)", compile_result.stdout)
    if match:
        error_count = int(match.group(1))
        if error_count > 0:
            return True
        else:
            return False
    else:
        return False

def is_failure_test( compile_result): 

    match = re.search(r"Failures:\s*(\d+)", compile_result.stdout)
    if match:
        error_count = int(match.group(1))
        if error_count > 0:
            return True
        else:
            return False
    else:
        return False

def is_run_test(compile_result):
        match = re.search(r"run:\s*(\d+)", compile_result.stdout)
        if match:
            run_count = int(match.group(1))
            if run_count > 0:
                return True
            else:
                return False
        else:
            return False

