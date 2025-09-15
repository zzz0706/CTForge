package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestDfsClientConf {

    @Test
    // test_getSlowIoWarningThresholdMs_defaultConfig
    // 1. Verify that the default configuration value of dfs.client.slow.io.warning.threshold.ms is correctly parsed and returned by getSlowIoWarningThresholdMs().
    // 2. Prepare the test conditions: Ensure the configuration uses default settings.
    // 3. Test code: Call getSlowIoWarningThresholdMs() on a DfsClientConf instance initialized with default configuration.
    // 4. Assert the expected result: The method should return 30000ms (default value).
    public void test_getSlowIoWarningThresholdMs_defaultConfig() {
        // Prepare the default configuration
        Configuration conf = new Configuration();

        // Initialize DfsClientConf instance
        DfsClientConf dfsClientConf = new DfsClientConf(conf);

        // Retrieve the slow I/O warning threshold value
        long slowIoWarningThresholdMs = dfsClientConf.getSlowIoWarningThresholdMs();

        // Assert the expected result
        assertEquals("Default threshold value does not match!", 30000, slowIoWarningThresholdMs);
    }
}