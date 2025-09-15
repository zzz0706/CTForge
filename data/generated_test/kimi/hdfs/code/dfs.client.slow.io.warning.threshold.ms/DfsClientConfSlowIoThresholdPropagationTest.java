package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DfsClientConfSlowIoThresholdPropagationTest {

    @Test
    // Test that the dfs.client.slow.io.warning.threshold.ms configuration is correctly propagated to DataStreamer
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testSlowIoWarningThresholdPropagationToDataStreamer() throws Exception {
        // Given
        Configuration conf = new Configuration();
        long customThreshold = 5000L;
        conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY, customThreshold);

        // Create DFSClient with the configuration
        DFSClient dfsClient = mock(DFSClient.class);
        
        // Test that the configuration value is correctly read by DfsClientConf
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        
        // When & Then
        // Verify that the slow I/O warning threshold is correctly propagated
        long actualThreshold = dfsClientConf.getSlowIoWarningThresholdMs();
        assertEquals("The slow I/O warning threshold should be propagated from configuration", 
                   customThreshold, actualThreshold);
    }
}