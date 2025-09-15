# MediConf

MediConf provides an end-to-end workflow for analysing configuration options in large Java projects and automatically generating JUnit tests with the help of large language models (LLMs).

## Running the workflow

### 1. Gather configuration flow information
1. Generate propagation paths with **cFlow**.
   The analysis writes source-to-sink paths to `tmp.txt`.
2. Summarise the paths and recover relevant source code.
   ```bash
   python  generation.extract_cflow.py    # build summaries for param
   python  generation.get_source_code.py  # get CR code
   ```

### 2. Generate tests
 `python generation.llm_test.py` compiles and executes the generated tests and coverage refinement.


### 3. Iterate on test quality

   `iteration/coverage.py` coverage refinement
 
1. Inject alternative configuration values and perform validity voting:
   ```bash
   python -m code.iteration.conf_inject.main hdfs
   ```
2. Perform mutation-based refinement:
   ```bash
   python -m code.iteration.mutation_testing hdfs
   ```
`pitest' is used for mutation testing.


## Data overview
The `data/` directory collects artefacts produced during experiments:
- `false positive/` – for each project (alluxio, hbase, hcommon, hdfs, zookeeper) this folder contains tests that were identified as false positives.
- `generated_test/` – LLM-generated test suites (`gpt-4o`, `kimi`).
- `issue/` –  code bugs, Incompatible Issue, and misconfigurations. A summary table lives in `real-world_issue.xlsx`.
- `misconfig_inject/` – injected misconfiguration values (`inject_value/`) and LLM evaluation results (`res/`).
- `param_data/` – Excel spreadsheets listing the configuration parameters of each studied project.

See [`data/README.md`](data/README.md) for details about each file.