# ExperimentCode

## Directory Structure

```
ExperimentCode/
├── extract_param_information/  
├── refinement/ 
├── pitest/ 
├── cflow/              
├── runCtestByConf/            
├── generated_tests/            
├── LLMPrompt.py              
├── LLMTest.py                  
├── Test_Generate.py            
├── config.py                  
├── path_construct.py           
└── prompt.py                 
```

` pitest ` provides a set of mutation operators (mutators) for Mutation-based Refinement.

` cflow` We build upon the openly released repository of cflow (https://github.com/xlab-uiuc/cflow), and optimize it in our work.

## Workflow

The workflow is split into dedicated packages so each stage can be used independently.
### 1. Gather configuration flow information
1. Generate propagation paths with **cFlow**.
   The analysis writes source-to-sink paths to `tmp.txt`.
2. Summarise the paths and recover relevant source code.
   `code/extract_param_information` provides the functionality to extract the relevant source code. 
   `data/param_data` contains the summaries for each parameter whicich is used to generate tests.

### 2. Generate Tests

First, prepare the configuration information. For example, the dataset in `data/param_data` can be used directly.  

Ensure that `config.py` is configured and that the target software is instrumented for **JaCoCo**.  

Next, set the `api-key` in the `LLMPrompt`.  

Finally, generation testing along with coverage refinement, by running:  

```bash
python LLMTest.py
```


### 3. Iterate on test quality

   `coverage.py` coverage refinement
   
   `conf_inject` validity voting
   
   `mutation_iteration.py` mutation-based refinement

During test generation, coverage refinement is already applied. Afterwards, conf_inject can be employed for validity voting through configuration value injection.
Once this step succeeds, mutation_iteration.py may be executed. Prior to mutation-based refinement, PIT must be integrated into the target software.

1. Inject alternative configuration values and perform validity voting:
   ```bash
   python conf_inject.main.py hdfs
   ```
2. Perform mutation-based refinement:
   ```bash
   python iteration.mutation_iteration.py hdfs
   ```


