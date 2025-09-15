# MediConf: Automatically Generating Configuration Tests for Large-scale Software Systems

## Environment Setup

### Software Stack
This project has been validated with the following stack (exact versions recommended):

- **OS:** Ubuntu 24.04  
- **Python:** 3.10.14  
- **JDK:** OpenJDK 1.8.0_452  
- **Maven:** Apache Maven 3.6.3  

### Pre-Run Requirements
Before execution, the target software must be properly instrumented for **JaCoCo**.  In particular, each module under test should include and configure JaCoCo in its `pom.xml`.  

In addition, the `code/pitest` module must be adapted to the target software, ensuring compatibility and avoiding dependency conflicts.  

### Required Source Code
The following software source code is required for experiments:

- **Hadoop 2.8.5**: `hadoop-common`, `hadoop-hdfs`  
- **HBase 2.2.2**: `hbase`  
- **ZooKeeper 3.5.6**: `zookeeper`  
- **Alluxio 2.1.0**: `core`  

## Demo

This `demo` is a simple example of **test generation**.  
It demonstrates how MediConf generates `Value validity testing` and `Config functional testing`.  


## Data Overview
The `data/` directory contains artifacts produced during experiments:

- **`false positive/`** – For each project (alluxio, hbase, hcommon, hdfs, zookeeper), this folder stores tests identified as false positives.  
- **`generated_test/`** – LLM-generated test suites (e.g., `gpt-4o`, `kimi`).  
- **`issue/`** – Includes code bugs, incompatibility issues, and misconfigurations. A summary table is provided in `real-world_issue.xlsx`.  
- **`misconfig_inject/`** – Contains injected misconfiguration values (`inject_value/`) and LLM evaluation results (`res/`).  
- **`param_data/`** – Excel spreadsheets listing configuration parameters of each studied project.  

For instructions on running the code, see [code/README.md](code/README.md).
For more details on the dataset, see [`data/README.md`](data/README.md).  
