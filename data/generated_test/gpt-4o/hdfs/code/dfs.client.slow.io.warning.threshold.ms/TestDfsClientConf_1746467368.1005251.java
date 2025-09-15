package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Assert;
import org.junit.Test;

public class TestDfsClientConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code to confirm that a custom configuration value for dfs.client.slow.io.warning.threshold.ms is correctly parsed and returned.
    // 4. Code after testing verifies functionality without error or misbehavior.
    public void test_getSlowIoWarningThresholdMs_customConfig() {
        // Prepare the configuration with a custom value for dfs.client.slow.io.warning.threshold.ms
        Configuration conf = new Configuration();
        long expectedThresholdMs = 10000; // Custom threshold value
        conf.setLong("dfs.client.slow.io.warning.threshold.ms", expectedThresholdMs);

        // Initialize the DfsClientConf instance using the custom configuration
        DfsClientConf dfsClientConf = new DfsClientConf(conf);

        // Test: Call getSlowIoWarningThresholdMs() and validate the result
        long actualThresholdMs = dfsClientConf.getSlowIoWarningThresholdMs();
        Assert.assertEquals("The parsed threshold value should match the configured value.", expectedThresholdMs, actualThresholdMs);
    }
}