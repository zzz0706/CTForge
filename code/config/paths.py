from pathlib import Path

# Default model for LLM interactions
LLM_MODEL = "gpt-4o"

# Repository root
BASE_DIR = Path(__file__).resolve().parents[2]

# Source code locations for supported projects
repo_path = {
    "hadoop": [BASE_DIR / "app" / "hadoop" / "hadoop-common-project" / "hadoop-common"],
    "hdfs": [BASE_DIR / "app" / "hadoop" / "hadoop-hdfs-project"],
    "hbase": [BASE_DIR / "app" / "hbase" / "hbase-server"],
    "alluxio": [BASE_DIR / "app" / "alluxio" / "core"],
    "zookeeper": [BASE_DIR / "app" / "zookeeper" / "zookeeper-server"],
}

# Configuration datasets
info_path = {
    "hadoop": BASE_DIR / "param_data" / "hadoop-common" / "hadoop_common.xlsx",
    "hdfs": BASE_DIR / "param_data" / "hdfs" / "hdfs.xlsx",
    "hbase": BASE_DIR / "param_data" / "hbase" / "hbase.xlsx",
    "alluxio": BASE_DIR / "param_data" / "alluxio" / "alluxio.xlsx",
    "zookeeper": BASE_DIR / "param_data" / "zookeeper" / "zookeeper.xlsx",
}

# Paths where tests are executed
execute_path = {
    "hadoop": BASE_DIR / "app" / "hadoop" / "hadoop-common-project" / "hadoop-common",
    "hdfs": BASE_DIR / "app" / "hadoop" / "hadoop-hdfs-project",
    "hbase": BASE_DIR / "app" / "hbase" / "hbase-server",
    "alluxio": BASE_DIR / "app" / "alluxio" / "core" ,
    "zookeeper": BASE_DIR / "app" / "zookeeper" / "zookeeper-server",
}

# Locations to store generated tests
store_path = {
    "hadoop": BASE_DIR / "generated_tests" / "hadoop" / "",
    "hdfs": BASE_DIR  / "generated_tests" / "hdfs" / "",
    "hbase": BASE_DIR  / "generated_tests" / "hbase" / "",
    "alluxio": BASE_DIR  / "generated_tests" / "alluxio" / "",
    "zookeeper": BASE_DIR  / "generated_tests" / "zookeeper" /  "",
}


