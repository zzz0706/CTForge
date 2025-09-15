package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Unit test for the TopConf class in HDFS 2.8.5, verifying proper instantiation and validation
 * of mixed configuration values (valid and invalid).
 */
public class TestTopConf {

    @Test
    // Test code for validating mixed configuration of dfs.namenode.top.windows.minutes values.
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions with a valid Hadoop Configuration object containing a mix of valid and invalid configuration values.
    // 3. Test the instantiation of TopConf and validate exception handling with proper assertions.
    // 4. Ensure no partial initialization takes place after exception handling.
    public void testMixedConfigurationValues() {
        // Step 1: Create configuration object and set dfs.namenode.top.windows.minutes.
        Configuration conf = new Configuration();
        String mixedValues = "1,-5,10"; // Contains both valid and invalid values.
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, mixedValues);

        // Step 2: Instantiate TopConf and observe any exceptions.
        try {
            new TopConf(conf);

            // Fail if no exception is thrown.
            fail("Expected IllegalArgumentException was not thrown for invalid configuration values.");
        } catch (IllegalArgumentException e) {
            // Step 3: Validate the exception's message.
            String expectedMessage = "minimum reporting period is 1 min!";
            if (!e.getMessage().contains(expectedMessage)) {
                fail("Exception message does not contain expected text: " + expectedMessage);
            }
        }

        // Step 4: Verify no partial initialization.
        // The initialization should halt completely if invalid configuration values are detected.
        // Since TopConf cannot be instantiated, no object exists, ensuring no partial initialization occurs.
    }
}