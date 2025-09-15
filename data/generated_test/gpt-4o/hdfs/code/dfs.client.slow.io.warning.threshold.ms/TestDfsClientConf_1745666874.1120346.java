package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDfsClientConf {

    // Test case for verifying the configuration value being correctly used
    @Test
    public void test_getSlowIoWarningThresholdMs_custom_value() {
        // Step 1: Create a Configuration object
        Configuration configuration = new Configuration();

        // Step 2: Set a custom value for dfs.client.slow.io.warning.threshold.ms
        long expectedValue = 5000L; // Setting a custom threshold of 5000ms
        configuration.setLong(HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY, expectedValue);

        // Step 3: Initialize DfsClientConf with the configuration
        DfsClientConf dfsClientConf = new DfsClientConf(configuration);

        // Step 4: Retrieve the slow I/O warning threshold value
        long actualValue = dfsClientConf.getSlowIoWarningThresholdMs();

        // Step 5: Assert the value is as expected
        assertEquals("Custom slow I/O warning threshold value does not match expectation", expectedValue, actualValue);
    }
}