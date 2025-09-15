import os



LLM_model = "kimi"

current_dir = os.path.dirname(os.path.abspath(__file__))

base_path = ""

# TODO target repo path
repo_path = {"hadoop": ["hadoop/hadoop-common-project/hadoop-common/"],
             "hdfs": ["hadoop/hadoop-hdfs-project/"],
             "hbase": ["hbase/hbase-server/"],
             "alluxio": ["alluxio/core/"],
             "zookeeper": ["zookeeper/zookeeper-server/"],
             }
# TODO param info path
info_path = {
    "hadoop" : "hadoop_common.xlsx",
    "hdfs" : "hdfs.xlsx",
    "hbase" : "hbase.xlsx",
    "alluxio" : "alluxio.xlsx",
    "zookeeper" : "zookeeper.xlsx"
}

# TODO target repo path for ctest
execute_path = {
    "hadoop" : "hadoop/hadoop-common-project/hadoop-common/",
    "hdfs" : "hadoop/hadoop-hdfs-project/",
    "zookeeper" : "zookeeper/zookeeper-server/"
}
# TODO store path for generated tests
store_path = {
    "hadoop" : "generated_tests/hadoop/",
    "hdfs" : "generated_tests/hdfs/",
    "hbase" : "generated_tests/hbase/",
    "alluxio" : "generated_tests/alluxio/",
    "zookeeper" : "generated_tests/zookeeper/ultimate/"
}
