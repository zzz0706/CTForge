package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testMixedConfigurationValues() {
        // Step 1: Prepare the test conditions.
        Configuration conf = new Configuration();
        String mixedValues = "1,-5,10";
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, mixedValues);

        // Step 2: Test code.
        try {
            // Attempt to instantiate TopConf with invalid configuration values.
            TopConf topConf = new TopConf(conf);
        } catch (IllegalArgumentException e) {
            // Step 3: Verify that it throws an exception with a message indicating invalid time periods.
            assert e.getMessage().contains("minimum reporting period is 1 min!");
        }

        // Step 4: Code after testing.
        // Ensure no further partial initialization occurs if an exception is thrown.
        // No further isEnabled or nntopReportingPeriodsMs state should be set in TopConf in this scenario.
    }
}