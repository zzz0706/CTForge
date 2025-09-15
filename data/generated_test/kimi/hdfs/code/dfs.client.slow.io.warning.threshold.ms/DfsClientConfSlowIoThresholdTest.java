package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DfsClientConfSlowIoThresholdTest {

    @Test
    public void testSlowIoWarningThresholdMsDefaultValue() {
        // Given
        Configuration conf = new Configuration();
        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        // Then
        assertEquals("Default value of dfs.client.slow.io.warning.threshold.ms should be 30000", 
                30000L, dfsClientConf.getSlowIoWarningThresholdMs());
    }

    @Test
    public void testSlowIoWarningThresholdMsCustomValue() {
        // Given
        Configuration conf = new Configuration();
        long expectedThreshold = 45000L;
        conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY, expectedThreshold);
        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        // Then
        assertEquals("Custom value of dfs.client.slow.io.warning.threshold.ms should match configured value",
                expectedThreshold, dfsClientConf.getSlowIoWarningThresholdMs());
    }

    @Test
    public void testConfigurationFileValueMatchesRuntime() {
        // Given
        Configuration conf = new Configuration();
        // Load default configurations that would be in hdfs-default.xml
        conf.addResource("hdfs-default.xml");

        // When
        long configValue = conf.getLong(
                HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
                HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT
        );

        // Then
        assertEquals("Configuration file value should match expected default",
                30000L, configValue);
    }
}