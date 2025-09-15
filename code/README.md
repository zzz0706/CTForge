
# Running the workflow

## 1. Gather configuration flow information
1. Generate propagation paths with **cFlow**.
   ```bash
   cd code/cflow
   mvn compile
   ./run.sh -a hdfs   # replace with target system
   ```
   The analysis writes source-to-sink paths to `tmp.txt`.
2. Summarise the paths and recover relevant source code.
   ```bash
   python -m code.generation.extract_cflow    # build summaries for param
   python -m code.generation.get_source_code  # pull method bodies
   ```
## 2. Generate tests
 `python -m code.generation.llm_test` compiles and executes the generated tests and coverage refinement.

Generated Java tests are stored under `data/generated_test/<llm>/<project>`.

## 3. Iterate on test quality

   `iteration/coverage.py` coverage refinement
 
1. Inject alternative configuration values and perform validity voting:
   ```bash
   python -m code.iteration.conf_inject.main hdfs
   ```
2. Perform mutation-based refinement:
   ```bash
   python -m code.iteration.mutation_testing hdfs
   ```
