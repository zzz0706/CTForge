package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDfsClientConf {

    // Prepare the input conditions for unit testing.
    @Test
    public void test_getSlowIoWarningThresholdMs_custom_value() {

        // Create a Configuration object
        Configuration configuration = new Configuration();

        // Retrieve the configuration value using the API
        long thresholdValue = configuration.getLong(
                HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
                HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);

        // Initialize DfsClientConf with the configuration
        DfsClientConf dfsClientConf = new DfsClientConf(configuration);

        // Invoke the getSlowIoWarningThresholdMs method
        long actualValue = dfsClientConf.getSlowIoWarningThresholdMs();

        // Assert that the method returns the custom threshold value
        assertEquals(thresholdValue, actualValue);
    }
}