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
        assertEquals("Default value of dfs.client.slow.io.warning.threshold.ms should be 30000 ms", 
                    30000L, dfsClientConf.getSlowIoWarningThresholdMs());
    }

    @Test
    public void testSlowIoWarningThresholdMsConfigurable() {
        // Given
        Configuration conf = new Configuration();
        long expectedThreshold = 45000L;
        conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY, expectedThreshold);
        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        // Then
        assertEquals("Configured value of dfs.client.slow.io.warning.threshold.ms should match", 
                    expectedThreshold, dfsClientConf.getSlowIoWarningThresholdMs());
    }

    @Test
    public void testSlowIoWarningThresholdLoadedFromFile() throws Exception {
        // Given
        Configuration conf = new Configuration();
        conf.addResource("hdfs-default.xml"); // Load from actual config file
        long defaultValue = 30000L; // Known default
        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        long actualValue = dfsClientConf.getSlowIoWarningThresholdMs();
        // Then
        assertEquals("Value loaded from hdfs-default.xml should match expected default", 
                    defaultValue, actualValue);
    }
}