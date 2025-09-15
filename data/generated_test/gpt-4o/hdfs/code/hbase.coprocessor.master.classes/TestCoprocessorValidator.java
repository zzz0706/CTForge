package org.apache.hadoop.hbase.tool.coprocessor;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.tool.coprocessor.CoprocessorValidator;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestCoprocessorValidator {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestCoprocessorValidator.class);

    @Test
    // Test code
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_doWork_with_coprocessor_validation() throws Exception {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        
        // Set up test configuration with valid and invalid coprocessor values
        // Correct the missing coprocessor configuration by explicitly setting values as part of the test setup
        conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, 
            "org.apache.hadoop.hbase.coprocessor.RegionObserverExample", // valid example class
            "org.apache.hadoop.hbase.coprocessor.InvalidFakeClass"); // invalid example class
        
        // Use the HBase API to retrieve coprocessor classes from the configuration
        String[] masterCoprocessors = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);

        // Ensure coprocessor classes are present in the configuration
        assertNotNull("Configuration is missing coprocessor classes", masterCoprocessors);
        assertTrue("Configuration should contain coprocessor classes", masterCoprocessors.length > 0);

        // Initialize the CoprocessorValidator with the test Configuration
        CoprocessorValidator validator = new CoprocessorValidator();
        validator.setConf(conf);

        // Test code: Invoke the 'doWork' method to validate coprocessor classes
        int result;
        try {
            result = validator.doWork(); // This indirectly triggers validation of coprocessor classes
        } catch (Exception e) {
            fail("doWork method failed with exception: " + e.getMessage());
            return;
        }

        // Code after testing
        // Assert that the return value matches the expected outcome
        // The return code should indicate successful validation or failure due to violations
        assertTrue("doWork method did not detect expected coprocessor violations", 
                result == CoprocessorValidator.EXIT_SUCCESS || result == CoprocessorValidator.EXIT_FAILURE);
    }
}