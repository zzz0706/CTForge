package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import java.util.concurrent.TimeUnit;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInvalidNonNumericValues() {
        // Step 1: Prepare the configuration object using the Hadoop configuration API
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "abc,5,25"); // Set invalid and valid values

        // Step 2: Prepare the test and verify conditions
        try {
            // Instantiate the TopConf class using the invalid configuration
            TopConf topConf = new TopConf(conf);
            // Fail the test if no exception is thrown
            assert false : "Expected NumberFormatException or IllegalArgumentException not thrown";
        } catch (NumberFormatException ex) {
            // Verify that the exception message contains the problematic input
            assert ex.getMessage() != null && ex.getMessage().contains("For input string: \"abc\"");
        } catch (IllegalArgumentException ex) {
            // Verify that the exception relates to an invalid configuration value
            assert ex.getMessage() != null && ex.getMessage().contains("Invalid configuration value");
        } catch (Exception ex) {
            // Fail the test if an unexpected exception type is thrown
            assert false : "Unexpected exception type thrown: " + ex.getClass().getName();
        }

        // Step 4: Clean up resources if necessary (not required in this case)
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Ensure valid configuration values are handled correctly.
    // 3. Prepare the test conditions and validate the code execution.
    // 4. Code after testing if cleanup is necessary, although not required in this scenario.
    public void testValidValues() {
        // Step 1: Prepare the configuration object using valid values
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "5,10,30"); // Set valid numeric values

        try {
            // Step 2: Instantiate the TopConf class
            TopConf topConf = new TopConf(conf);

            // Step 3: Verify the internal state of the TopConf instance
            int[] expectedPeriodsMs = {
                (int) TimeUnit.MINUTES.toMillis(5),
                (int) TimeUnit.MINUTES.toMillis(10),
                (int) TimeUnit.MINUTES.toMillis(30)
            };

            assert topConf.nntopReportingPeriodsMs.length == expectedPeriodsMs.length;
            for (int i = 0; i < expectedPeriodsMs.length; i++) {
                assert topConf.nntopReportingPeriodsMs[i] == expectedPeriodsMs[i] : 
                    "Mismatch in reporting period at index " + i;
            }
        } catch (Exception ex) {
            // Fail the test if any exception is thrown
            assert false : "Unexpected exception thrown: " + ex.getClass().getName();
        }

        // Step 4: Clean up resources if necessary (not required in this case)
    }
}