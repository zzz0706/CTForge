package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.junit.Test;
import static org.junit.Assert.fail;

/**
 * Unit test for the TopConf class in HDFS 2.8.5, ensuring proper instantiation and validation
 * of mixed configuration values (valid and invalid).
 */
public class TestTopConf {

    @Test
    // Test code for validating mixed configuration of dfs.namenode.top.windows.minutes values.
    // 1. Use HDFS 2.8.5 API to set and retrieve configuration values correctly.
    // 2. Prepare the test conditions with the configuration containing mixed values.
    // 3. Test the instantiation of TopConf and validate exception handling.
    // 4. Verify no partial initialization occurs after exception handling.
    public void testMixedConfigurationValues() {
        // Step 1: Prepare the test conditions.
        // Creating a configuration object and setting the dfs.namenode.top.windows.minutes property.
        Configuration conf = new Configuration();
        String mixedValues = "1,-5,10"; // Configuration contains both valid and invalid values.
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, mixedValues);

        // Step 2: Execute the test.
        try {
            // Attempt to instantiate TopConf, which processes the configuration values.
            new TopConf(conf);

            // Fail the test if no exception is thrown, since we expect validation errors.
            fail("Expected IllegalArgumentException was not thrown for invalid configuration values.");
        } catch (IllegalArgumentException e) {
            // Step 3: Verify the behavior.
            // Check that the exception contains the expected message about invalid values.
            String expectedMessage = "minimum reporting period is 1 min!";
            if (!e.getMessage().contains(expectedMessage)) {
                fail("Exception message does not contain expected text: " + expectedMessage);
            }
        }

        // Step 4: Post-testing verification.
        // Validate that the TopConf object is not partially initialized due to thrown exceptions.
        // No additional verification is needed, as TopConf fails on instantiation for invalid values.
    }
}