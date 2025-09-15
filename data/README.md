# Data directory

This directory stores datasets used and produced by CTForge.

- `false positive/` – for each project (alluxio, hbase, hcommon, hdfs, zookeeper) this folder contains tests that were identified as false positives.
- `generated_test/` – LLM-produced test suites grouped by model (`gpt-4o`, `kimi`) and project.
- `issue/` – reproduction tests and metadata for real-world problems:
  - `bug/` – tests that expose known defects.
  - `code_change/` – cases showing behaviour differences after code modifications.
  - `misconf/` – tests covering misconfiguration scenarios.
  - `real-world_issue.xlsx` – summary table of reported issues.
- `misconfig_inject/` – assets for configuration injection experiments:
  - `inject_value/` – CSV files of alternative configuration values.
  - `res/` – JSON results from LLM evaluation for each model and project.
- `param_data/` – Excel spreadsheets listing configuration parameters for each studied project.

Project names in the subdirectories follow a consistent naming scheme (e.g. `alluxio`, `hbase`, `hcommon`, `hdfs`, `zookeeper`). Test files and metadata are organised under these project-level folders.