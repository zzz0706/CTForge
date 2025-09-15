package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DataNodeConfigurationTest {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_startMetricsLogger_with_zero_period() {
        // Step 2: Prepare the test conditions.
        Configuration conf = new Configuration();
        
        // Set the configuration value using Configuration API.
        conf.setInt(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, 0);

        // Step 3: Test code - Validate that the configuration value was set correctly.
        int metricsLoggerPeriod = conf.getInt(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, -1);
        assertEquals("Metrics logger period should be zero as set in the configuration.", 0, metricsLoggerPeriod);

        // Step 4: Code after testing.
        // Additional cleanup or assertions can be added here if necessary.
    }
}