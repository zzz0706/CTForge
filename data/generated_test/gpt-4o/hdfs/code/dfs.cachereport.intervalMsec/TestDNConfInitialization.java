package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDNConfInitialization {

    @Test
    // test_DNConf_initialization_with_valid_configuration
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDNConfInitializationWithValidConfiguration() {
        // Prepare the test conditions: Create a Configuration instance.
        Configuration configuration = new Configuration();
        
        // Extract the value of 'dfs.cachereport.intervalMsec' using the API (no hardcoding).
        long expectedCacheReportInterval = configuration.getLong(
            DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 
            DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT
        );

        // Test code: Initialize the DNConf instance.
        DNConf dnConf = new DNConf(configuration);

        // Validate that the cacheReportInterval value in DNConf matches the expected value from the configuration.
        assertEquals("DNConf did not correctly initialize the cacheReportInterval value.",
                     expectedCacheReportInterval, dnConf.cacheReportInterval);
    }
}