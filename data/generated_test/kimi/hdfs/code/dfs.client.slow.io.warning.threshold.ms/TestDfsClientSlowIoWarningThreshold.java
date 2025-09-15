package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestDfsClientSlowIoWarningThreshold {

    private Configuration conf;
    private DfsClientConf dfsClientConf;

    @Before
    public void setUp() {
        conf = new Configuration();
        dfsClientConf = new DfsClientConf(conf);
    }

    @Test
    public void testSlowIoWarningThresholdMsDefaultValue() {
        // Given: No explicit configuration set
        // When: Get the default value from DfsClientConf
        long actualValue = dfsClientConf.getSlowIoWarningThresholdMs();

        // Then: Should match the documented default (30000 ms)
        assertEquals(30000L, actualValue);
    }

    @Test
    public void testSlowIoWarningThresholdMsCustomValue() {
        // Given: Custom configuration value
        long customThreshold = 50000L;
        conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY, customThreshold);

        // When: Create new DfsClientConf with updated config
        DfsClientConf customConf = new DfsClientConf(conf);

        // Then: Should reflect the custom value
        assertEquals(customThreshold, customConf.getSlowIoWarningThresholdMs());
    }

    @Test
    public void testConfigurationKeyConsistency() {
        // Verify that the constant matches the expected string key
        assertEquals("dfs.client.slow.io.warning.threshold.ms",
                HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY);
    }

    @Test
    public void testCompareWithRawConfigurationAccess() {
        // Given: A configuration with a specific value
        long testValue = 45000L;
        conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY, testValue);

        // When: Access via DfsClientConf API
        DfsClientConf dfsConf = new DfsClientConf(conf);
        long viaApi = dfsConf.getSlowIoWarningThresholdMs();

        // And: Access directly from Configuration
        long viaRawConf = conf.getLong(
                HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
                DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);

        // Then: Both should be equal
        assertEquals(testValue, viaApi);
        assertEquals(testValue, viaRawConf);
    }
}