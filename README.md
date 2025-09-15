# MediConf: Automatically Generating Configuration Tests For Large-scale Software Systems

This is a repo for MediConf(FSE26 submission)

## Running the workflow
The workflow is split into dedicated packages so each stage can be used independently.
### 1. Gather configuration flow information
1. Generate propagation paths with **cFlow**.
   The analysis writes source-to-sink paths to `tmp.txt`.
2. Summarise the paths and recover relevant source code.
   ```bash
   python  generation.get_source_code.py  # get CR code
   python  generation.extract_cflow.py    # build summaries for param
   ```

### 2. Generate tests
Integrate JaCoCo into the target software, and then execute the following command.

    python generation.llm_test.py

 `python generation.llm_test.py` compiles and executes the generated tests and coverage refinement.


### 3. Iterate on test quality

   `iteration/coverage.py` coverage refinement
   
   `iteration/conf_inject.py` validity voting
   
   `iteration/mutation_testing.py` mutation-based refinement
 
1. Inject alternative configuration values and perform validity voting:
   ```bash
   python iteration.conf_inject.main.py hdfs
   ```
2. Perform mutation-based refinement:
   `Pitest ` is a mutation operator used for mutation testing. Apply it to the target project and then execute the following command: 
   ```bash
   python iteration.mutation_testing.py hdfs
   ```
 


## Data overview
The `data/` directory collects artefacts produced during experiments:
- `false positive/` – for each project (alluxio, hbase, hcommon, hdfs, zookeeper) this folder contains tests that were identified as false positives.
- `generated_test/` – LLM-generated test suites (`gpt-4o`, `kimi`).
- `issue/` –  Code Bugs, Incompatible Issue, and Misconfigurations. A summary table lives in `real-world_issue.xlsx`.
- `misconfig_inject/` – injected misconfiguration values (`inject_value/`) and LLM evaluation results (`res/`).
- `param_data/` – Excel spreadsheets listing the configuration parameters of each studied project.

See [`data/README.md`](data/README.md) for details about each file.