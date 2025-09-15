# Data directory

This directory stores datasets used and produced by CTForge.

- `false positive/` – for each project (alluxio, hbase, hcommon, hdfs, zookeeper) this folder contains tests that were identified as false positives. RQ3: Validity Voting.
- `generated_test/` – LLM-produced test suites grouped by model (`gpt-4o`, `kimi`) and project.
  - `conf/` – Value validity testing.
  - `code/` – Config functional testing..
- `issue/` – reproduction tests and metadata for real-world problems, RQ1:
  - `bug/` – code bugs.
  - `code_change/` – cases showing behaviour differences after code modifications.
  - `misconf/` – tests covering misconfiguration scenarios.
  - `real-world_issue.xlsx` – summary table of reported issues. RQ3:Mutation-based Refinement.
- `misconfig_inject/` – assets for configuration injection experiments, RQ2:
  - `inject_value/` – CSV files of alternative configuration values.
  - `res/` – JSON results from LLM evaluation for each model and project.
- `param_data/` –  Configuration information for generating tests.

