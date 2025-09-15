package org.apache.hadoop.hdfs.client.impl;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestDfsClientConf {       
    // Test configuration value retrieval using the public API
    @Test
    public void test_getSlowIoWarningThresholdMs_default_value() {
        // 1. Prepare the configuration
        Configuration conf = new Configuration();

        // 2. Retrieve the default value using the `HdfsClientConfigKeys` API
        long defaultValue = conf.getLong(
            HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
            HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT
        );

        // 3. Verify that the default value matches the expected default
        long expectedDefaultValue = HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT;
        assertEquals(expectedDefaultValue, defaultValue);
    }
}