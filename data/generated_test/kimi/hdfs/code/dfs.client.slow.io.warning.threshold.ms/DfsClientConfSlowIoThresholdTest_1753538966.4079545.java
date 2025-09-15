package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class DfsClientConfSlowIoThresholdTest {

    private long expectedThreshold;

    public DfsClientConfSlowIoThresholdTest(long expectedThreshold) {
        this.expectedThreshold = expectedThreshold;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {10000L}, {30000L}, {60000L}, {120000L}
        });
    }

    @Test
    public void testSlowIoWarningThresholdDefault() throws Exception {
        // Given
        Configuration conf = new Configuration();
        // Do not set the property to test default value

        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);

        // Then
        assertEquals("Default value of dfs.client.slow.io.warning.threshold.ms should be 30000", 
                    30000L, dfsClientConf.getSlowIoWarningThresholdMs());
    }

    @Test
    public void testSlowIoWarningThresholdConfigured() throws Exception {
        // Given
        Configuration conf = new Configuration();
        conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY, expectedThreshold);

        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);

        // Then
        assertEquals("Configured value of dfs.client.slow.io.warning.threshold.ms should match", 
                    expectedThreshold, dfsClientConf.getSlowIoWarningThresholdMs());
    }

    // Removed DataStreamer test as DataStreamer is not accessible from outside package
    // and reflection-based access would be fragile

    @Test
    public void testSlowIoWarningThresholdFromFile() throws IOException {
        // Given
        Configuration conf = new Configuration();
        // This will load defaults from the configuration files
        conf.addResource("hdfs-default.xml");

        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);

        // Then
        // Compare against the constant defined in the codebase
        assertEquals("Value loaded from hdfs-default.xml should match the default constant",
                    HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT,
                    dfsClientConf.getSlowIoWarningThresholdMs());
    }
}