
demo_summary = """


"""

demo_validity_testing = """

Given the configuration information for Hadoop-Common 2.8.5, please understand the constraints and dependencies between configurations, and write test code to detect erroneous configurations. The requirement is not to set configuration values within the test code.
Return the complete test code, without returning any other information.


target software:Hadoop-Common 2.8.5
You should consider the unit testing guidelines for Hadoop-Common 2.8.5, and then correctly generate the test code.

The configuration information is as follows. For the configuration information, you need to understand the functionality of the configuration and its value constraints.


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


You need to understand the rules for writing unit tests in Hadoop-Common 2.8.5 and pay attention to the writing of comments.

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
     // 1. You need to use the Hadoop-Common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
```

"""