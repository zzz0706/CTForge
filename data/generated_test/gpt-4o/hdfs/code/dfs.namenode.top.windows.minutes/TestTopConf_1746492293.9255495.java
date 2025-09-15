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
    public void testInvalidConfigurationWithZeroMinutes() {
        // Prepare the test conditions
        Configuration conf = new Configuration(); 
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "0,5,25");

        // Test
        try {
            new TopConf(conf);
        } catch (IllegalArgumentException e) {
            // Expected result: an IllegalArgumentException is thrown with the proper message
            assert e.getMessage().contains("minimum reporting period is 1 min");
            return; // Test passes
        }

        // Code after testing: fail the test if no exception was thrown
        assert false : "Expected IllegalArgumentException was not thrown.";
    }
}