
repo_name = "repo_name"

generate_system_role = f"""You are a professional software engineer who understands Java projects and various configuration items, and you know how to write professional test code."""


mutation_system_role = f"""You are a professional JAVA engineer. Please improve unit test code based on {repo_name}"""

mutation_prompt = """The following test code failed to kill any mutants. Rewrite the test to better exercise the configuration
and trigger faults in the related code paths while keeping the code compilable and executable.

target software: repo_name

// exist test code
{test_code}

The configuration information is as follows. You need to understand the functionality of the configuration and its value constraints.
{conf_info}

The propagation summary of this configuration in the project is as follows. You need to understand how the configuration propagates and is used.
{data_flow_summary}

The configuration's usage within the source code is outlined below.
{source_code}

The functions related to the configuration's functionality are listed below. When generating test cases, please ensure that you can reach or use the following functions.
{function_method}

Please return strictly according to the template, and do not output any information other than the code.
```java
package <package_name>

import <module_1>
import <module_2>
import <module_3>

public class <class_name>
    @test
    //test code
    // 1. You need to use the repo_name API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

```
"""



compile_failure_system_role = f"""You are a professional JAVA engineer. Please write unit test code based on repo_name"""

compile_failure_prompt = """Below is the test code. It has some compilation errors or errors in test code. Please modify the test code according to the compilation information to make it compile successfully. Only return the complete test code.Please return strictly according to the template.
If there are compilation errors in the test code, consider how to correct the code to ensure it compiles correctly. If there are runtime errors, think about the cause of the error, whether the logic in the code is incorrect, and if any prerequisites are missing before test code.

target software:repo_name

// test code
{test_code}
//test information
{compile_information}
// relate source code
{source_code}

You need to think about why it went wrong, then find the cause in the source code, and then modify the test code to ensure that the code is executable.


You need to understand the rules for writing unit tests in repo_name and pay attention to the writing of comments.


Please return strictly according to the template, and do not output any information other than the code.
// return template
```java
package <package_name> 

import <module_1>       
import <module_2>
import <module_3>

public class <class_name>

  @test
 //test code
     // 1. You need to use the repo_name API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
```
"""

mis_conf_system_role = f"""You are a professional JAVA engineer. Please write unit test code based on repo_name"""

mis_conf_constrain_prompt = """Given the configuration information for repo_name, please understand the constraints and dependencies between configurations, and write test code to detect erroneous configurations. The requirement is not to set configuration values within the test code.
Return the complete test code, without returning any other information.


target software:repo_name
You should consider the unit testing guidelines for repo_name, and then correctly generate the test code.

The configuration information is as follows. For the configuration information, you need to understand the functionality of the configuration and its value constraints.
{conf_info}

The propagation summary of this configuration in the project is as follows. You need to understand how the configuration propagates through the functions and how it plays its role.
{data_flow_summary}

The configuration's usage within the source code is outlined below. Note that during its propagation, it might be influenced or constrained by other configurations. You should analyze the code to understand how this configuration is utilized and how it achieves its intended purpose. Additionally, consider the specific workloads related to the functionality provided by this configuration.
{source_code}

The functions related to the configuration's functionality are listed below. When generating test cases, please ensure that you can reach or use the following functions.
{function_method}

Note:
1. You need to understand the constraints of the configuration and its valid values from the configuration information and the source code.
2. You need to understand which configurations have dependencies with this configuration by examining the source code and the configuration dependencies.
3. Generate test code to read the relevant configurations from the configuration file and determine whether the configurations in the file are valid.
4. The test code only needs to determine whether the configuration value is valid by checking if it satisfies the constraints and dependencies.


You need to understand the rules for writing unit tests in repo_name and pay attention to the writing of comments.

Here is the English translation of your text:

* Step 1: Based on the understood constraints and dependencies, determine whether the retrieved configuration value satisfies those constraints and dependencies.  
* Step 2: Verify whether the value of the configuration item meets the constraints and dependencies.  
//1. For enumeration or boolean types, directly check whether the obtained configuration value is one of the allowed values.  
//2. For range-based values, check that the value falls within the specified range and matches the expected data type (e.g., an int should not be a floating-point number).  
//3. For ports or IP addresses, use constraint checks to validate them.  
//4. For path-type configurations, verify whether the path is valid or use constraint checks.  
//5. For other types that are difficult to validate using constraints, you can understand how the configuration is used by examining the source code.  
//6. Some configurations have dependency relationships, such as min/max value dependencies or control dependencies. You need to understand these from the source code.

Please return strictly according to the template, and do not output any information other than the code.
// return template
```java
package <package_name> 

import <module_1>       
import <module_2>
import <module_3>

public class <class_name>
//Just one or two test methods is needed to verify if the set configuration value is valid. By understanding the constraints, it's possible to determine dependencies between multiple values and the valid range of a configuration value.
  @test
 //test code
     // 1. You need to use the repo_name API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
```
```
"""



test_case_code = """
You are a Java test engineer tasked with generating automated configuration tests for a project. All configuration values live in external files and are accessed at runtime via a `ConfigService` interface (e.g. `long getLong(String key)`, `String getString(String key)`, `boolean getBoolean(String key)`).

**Target Software:**
target software: repo_name

**Configuration Information:**
{conf_info}

**Configuration Usage Explanation:**
{data_flow_summary}

**Configuration-Related Code:**
```java
{source_code}
```

Now you have understood the constraints, functionality, purpose, dependencies, and usage of this configuration in the software. Please help me write test code to test the configuration based on the test case.
{test_case}

Please generate a single, complete Java test method based on the above scenario. The method must emphasize and follow these key points:

1. **Configuration as Input**  
   - Instantiate `Configuration conf = new Configuration();` inside the `@Test` method  
   - **Do not call** `conf.set(...)` so that defaults or test‑resource overrides are used  

2. **Dynamic Expected Value Calculation**  
   - Use `conf.getXxx(key, default)` to read the configuration  
   - Compute `long expectedXxx = …;` according to the logic described in the scenario  

3. **Mock/Stub External Dependencies**  
   - Use PowerMock/Mockito to mock static calls (e.g., `Thread.sleep`) or constructors  
   - Stub any other external interactions (RPC clients, HTTP calls, database, etc.)  

4. **Invoke the Method Under Test**  
   - Call the target method or entry point, passing in `conf` as needed  

5. **Assertions and Verification**  
   - For external calls: use `verifyStatic()` or `verify(mock).method(argument)` to check parameters  
   - For return values or state: use `assertEquals(expectedXxx, actualXxx)`

**Output:**
Please return strictly according to the template, and do not output any information other than the code.
// return template
```java
package <package_name> 
import <module_1>       
import <module_2>
import <module_3>
public class <class_name>       
 
    @test
    // test code
    // 1. You need to use the repo_name API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
```


"""




test_case_prompt = """
You are an expert Java test engineer, proficient in software configuration and configuration testing. You will produce multiple test-case specifications to guide the generation of test code. Follow this workflow step by step:

**Target Software:**
target software: repo_name

**Configuration Information:**
{conf_info}

**Configuration Usage Explanation:**
{data_flow_summary}

**Configuration-Related Code:**
```java
{source_code}
```

1. Treat `Configuration` as an input rather than a constant
   * In your test, create a new `Configuration` instance **without** calling `conf.set(...)`, so it uses defaults or test‑resource overrides.
   * This way, tests automatically adapt if defaults or resource‑provided values change.
2. Dynamically compute expected values
   * Use `conf.getXxx(key, default)` to read the configuration.
   * Based on the code’s logic (for example `sleepTime = interval * 2000 + retry * 1000`, or `limit = conf.getInt(...)`), compute an `expectedXxx` value at runtime.
3. Cover two main scenario types
   * **External Function Call Type**
     * Examples: `Thread.sleep`, HTTP client timeouts, database retry backoff.
     * Verify that the external function is called with the dynamically computed `expectedXxx`.
     * Approach:
       1. Use PowerMock/Mockito to mock static calls (e.g., `mockStatic(Thread.class)`) or constructors.
       2. Invoke the entry point under test (e.g., `MyService.run(conf)`).
       3. Use `verifyStatic()` or `verify(...)` to assert the call parameter.
   * **Conditional Branch Type**
     * Examples: loop limits, batch sizes, feature flags, threshold checks.
     * Verify that the number of iterations or which branch is taken matches the configuration.
     * Approach:
       1. Compute the expected loop count or branch decision from the configuration.
       2. Mock business calls or iterators and count invocations.
       3. Use `assertEquals()`, `verify(...)`, or log assertions to confirm behavior.
4. Mock and stub all external dependencies
   * Intercept any network, I/O, thread, or RPC calls so tests focus solely on configuration‑driven logic.
   * Stub static methods (`Thread.sleep`), constructors (`whenNew(...)`), and factory methods as needed.
5. Use a complete test structure
   * Include a `@Before` setup method and one or more `@Test` methods.
   * Use the appropriate JUnit runner (PowerMockRunner or Mockito JUnit Runner).
   * Name tests clearly, e.g., `testSleepTimeRespectsConfig()` or `testLimitRespectsConfig()`.
6. Avoid magic numbers
   * Compute all expected values from the `Configuration` at runtime; do not hard‑code any constants.
   * This improves maintainability and ensures tests adapt to configuration changes.
7. Assertions and verification
   * For external calls: use `verifyStatic()` plus parameter checks or `verify(mock).method(arg)`.
   * For return values or state: use `assertEquals(expected, actual)`.
   * Optionally, capture and assert log output if needed.

**Output:**
Return only a JSON array of test-case explanations in this exact template—no other output:
// return array template example:
```json
[{test_case_name
objective
prerequisites
steps
expected_result},
{test_case_name
objective
prerequisites
steps
expected_result}]
```

"""



coverage_system_role = """
You are an excellent software engineer who has participated in the development of Java projects such as Hadoop, hadoop-common, HBase, Alluxio, and Zookeeper, and you have a deep understanding of them.
"""

coverage_prompt = """
The test cases I have written currently do not effectively cover the testing code for the code below. Please provide useful information to help me maximize coverage of the relevant code while ensuring that the test code is correct and executable. First, you need to identify the parts of the code that are related to the test cases, and then generate code to ensure that those parts are covered.

Please rewrite the test code based on the test cases to ensure coverage of the configuration usage. First, ensure that the test code can be compiled, executed, and functions correctly.

target software: repo_name

//test case
{test_case}

//test code not coverage configuration usage
{test_code}

The following summary explains how the configuration propagates and is used within the software. You can understand how the configuration propagates and is used, from parsing, propagation, usage, to being invoked in a scenario.
{data_flow_summary}

Here are some specific function names related to the use of the configuration. You need to generate test cases for the functions in the usage phase (public methods), ensuring that these functions are genuinely utilized. In each of the following files, typically the public methods call the private methods. You can cover the private methods by invoking the public methods and ensuring that the calls to the private methods within the public methods are executed.
{function_method}

// relate source code
{source_code}


You need to understand the rules for writing unit tests in repo_name and pay attention to the writing of comments.


Please return strictly according to the template, and do not output any information other than the code.
// return template
```java
package <package_name> 

import <module_1>       
import <module_2>
import <module_3>


public class <class_name>       

    @test
    // test code
    // 1. You need to use the repo_name API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
```
"""