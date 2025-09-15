package org.apache.hadoop.hdfs.server.datanode; 

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.DFSConfigKeys;       
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertNull;

public class TestDataNodeConfiguration {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_startMetricsLogger_with_negative_period() throws IOException {
        // Step 1: Prepare the test conditions.
        Configuration conf = new Configuration();
        // Set necessary configuration values.
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:9000");

        // Use the API to set dfs.datanode.metrics.logger.period.seconds to a negative value (-1).
        conf.setInt(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, -1);

        // Step 2: Initialize a DataNode instance with necessary parameters.
        SecureDataNodeStarter.SecureResources secureResources = null;  // Null for this test
        DataNode dataNode = DataNode.instantiateDataNode(
                new String[]{}, conf, secureResources);

        dataNode.runDatanodeDaemon();

        // Step 3: Call the startMetricsLogger method.
        dataNode.startMetricsLogger(conf);

        // Step 4: Assert the expected result.
        // Since the configuration value is negative, no periodic task should be scheduled.
        ScheduledThreadPoolExecutor metricsLoggerTimer = dataNode.getMetricsLoggerTimer();
        assertNull("No ScheduledThreadPoolExecutor should be initialized.", metricsLoggerTimer);
    }
}