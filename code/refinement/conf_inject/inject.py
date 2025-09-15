"""inject parameter, values into sw config"""

import sys
import xml.etree.ElementTree as ET
import os


CTEST_HADOOP_DIR = "hadoop/"
CTEST_HBASE_DIR = "hbase/"
CTEST_ZK_DIR = "zookeeper/"
zookeeper_dir = "zookeeper"
CTEST_ALLUXIO_DIR = "alluxio/"
# injecting config file location
inject_repo_path = {
    "hadoop": [
        os.path.join(CTEST_HADOOP_DIR, "hadoop-common-project/hadoop-common/target/classes/core-ctest.xml")
    ],
    "hdfs": [
        os.path.join(CTEST_HADOOP_DIR, "hadoop-hdfs-project/hadoop-hdfs/target/classes/core-ctest.xml"),
        os.path.join(CTEST_HADOOP_DIR, "hadoop-hdfs-project/hadoop-hdfs/target/classes/hdfs-ctest.xml")
    ],
    "hbase": [
        os.path.join(CTEST_HBASE_DIR, "hbase-server/target/classes/core-ctest.xml"),
        os.path.join(CTEST_HBASE_DIR, "hbase-server/target/classes/hbase-ctest.xml")
    ],
    "zookeeper": [
        os.path.join(zookeeper_dir, "zookeeper-server/ctest.cfg")
    ],
    "alluxio": [
        os.path.join(CTEST_ALLUXIO_DIR, "core/alluxio-ctest.properties")
    ]
}

CTEST_valid_HADOOP_DIR = "hadoop/"
CTEST_valid_HBASE_DIR = "hbase/"
CTEST_valid_ZK_DIR = "zookeeper/"
CTEST_valid_ALLUXIO_DIR = "alluxio/"

inject_valid_repo_path = {
    "hadoop": [
        os.path.join(CTEST_valid_HADOOP_DIR, "hadoop-common-project/hadoop-common/target/classes/core-ctest.xml")
    ],
    "hdfs": [
        os.path.join(CTEST_HADOOP_DIR, "hadoop-hdfs-project/hadoop-hdfs/target/classes/core-ctest.xml"),
        os.path.join(CTEST_HADOOP_DIR, "hadoop-hdfs-project/hadoop-hdfs/target/classes/hdfs-ctest.xml")
    ],
    "hbase": [
        os.path.join(CTEST_HBASE_DIR, "hbase-server/target/classes/core-ctest.xml"),
        os.path.join(CTEST_HBASE_DIR, "hbase-server/target/classes/hbase-ctest.xml")
    ],
    "zookeeper": [
        os.path.join(CTEST_valid_ZK_DIR, "zookeeper-server/ctest.cfg")
    ],
    "alluxio": [
        os.path.join(CTEST_ALLUXIO_DIR, "core/alluxio-ctest.properties")
    ]
}

def inject_value_config(repo_name, param_value_pairs):
    for p, v in param_value_pairs.items():
        print(">>>>[ctest_core] injecting {} with value {}".format(p, v))

    if repo_name in ["zookeeper", "alluxio"]:
        for inject_path in inject_valid_repo_path[repo_name]:
            print(">>>>[ctest_core] injecting into file: {}".format(inject_path))
            file = open(inject_path, "w")
            for p, v in param_value_pairs.items():
                file.write(p + "=" + v + "\n")
            file.close()
    elif repo_name in ["hadoop", "hdfs", "hbase"]:
        conf = ET.Element("configuration")
        print(f"pro:{repo_name}")
        for p, v in param_value_pairs.items():
            prop = ET.SubElement(conf, "property")
            name = ET.SubElement(prop, "name")
            value = ET.SubElement(prop, "value")
            name.text = p
            value.text = v
        for inject_path in inject_valid_repo_path[repo_name]:
            print(">>>>[ctest_core] injecting into file: {}".format(inject_path))
            file = open(inject_path, "wb")
            file.write(str.encode("<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"))
            file.write(ET.tostring(conf))
            file.close()
    else:
        sys.exit(">>>>[ctest_core] value injection for {} is not supported yet".format(repo_name))

def inject_config(repo_name, param_value_pairs):
    for p, v in param_value_pairs.items():
        print(">>>>[ctest_core] injecting {} with value {}".format(p, v))

    if repo_name in ["zookeeper", "alluxio"]:
        for inject_path in inject_repo_path[repo_name]:
            print(">>>>[ctest_core] injecting into file: {}".format(inject_path))
            file = open(inject_path, "w")
            for p, v in param_value_pairs.items():
                file.write(p + "=" + v + "\n")
            file.close()
    elif repo_name in ["hadoop", "hdfs", "hbase"]:
        conf = ET.Element("configuration")
        print(f"pro:{repo_name}")
        for p, v in param_value_pairs.items():
            prop = ET.SubElement(conf, "property")
            name = ET.SubElement(prop, "name")
            value = ET.SubElement(prop, "value")
            name.text = p
            value.text = v
        for inject_path in inject_repo_path[repo_name]:
            print(">>>>[ctest_core] injecting into file: {}".format(inject_path))
            file = open(inject_path, "wb")
            file.write(str.encode("<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"))
            file.write(ET.tostring(conf))
            file.close()
    else:
        sys.exit(">>>>[ctest_core] value injection for {} is not supported yet".format(repo_name))


def clean_conf_file(repo_nem):
    print(">>>> cleaning injected configuration from file")
    if repo_nem in ["zookeeper", "alluxio"]:
        for inject_path in inject_repo_path[repo_nem]:
            file = open(inject_path, "w")
            file.write("\n")
            file.close()
    elif repo_nem in ["hadoop", "hdfs", "hbase"]:
        conf = ET.Element("configuration")
        for inject_path in inject_repo_path[repo_nem]:
            file = open(inject_path, "wb")
            file.write(str.encode("<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"))
            file.write(ET.tostring(conf))
            file.close()
    else:
        sys.exit(">>>>[ctest_core] value injection for {} is not supported yet".format(repo_nem))
