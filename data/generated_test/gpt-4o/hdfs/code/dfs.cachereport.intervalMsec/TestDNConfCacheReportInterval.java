package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDNConfCacheReportInterval {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_DNConf_cacheReportInterval_custom_value() {
        // 1. Prepare the test conditions: Create a Configuration object and set a custom value for dfs.cachereport.intervalMsec.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 20000);

        // 2. Instantiate the DNConf object using the prepared Configuration object.
        DNConf dnConf = new DNConf(conf);

        // 3. Test code: Access the cacheReportInterval field and ensure it matches the custom value set in the configuration.
        long cacheReportInterval = dnConf.getConf().getLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT); // Correctly access the configuration value.

        // 4. Assert the expected result.
        assertEquals("The cacheReportInterval should be initialized to the custom value specified in the configuration.", 20000, cacheReportInterval);
    }
}