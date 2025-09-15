This module groups the refactored code for analysing configuration options and
automatically generating tests.  The workflow is split into dedicated
packages so each stage can be used independently.

## Package overview
- `config/` – repository paths and shared constants.
- `generation/` – utilities to collect source code, query an LLM for
  configuration summaries and build test cases.
- `iteration/` – helpers for improving tests through coverage inspection,
  configuration value injection and mutation analysis.

## Typical workflow
1. **Extract configuration information**
   - `generation/get_source_code.py` .
   - `generation/extract_cflow.py` 
2. **Generate tests**
   - `generation/test_generate.py` creates candidate test cases.
   - `generation/llm_test.py` compiles and runs the tests.
3. **Iterate on quality**
   - `iteration/coverage.py` Coverage Refinement.
   - `iteration/conf_inject.py` Validity Voting, injects new configuration values into source files.
   - `iteration/mutation_testing.py` Mutation-based Refinement.


