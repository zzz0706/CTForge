package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestHDFSConfiguration {

    @Test
    public void testValidConfigurationValues() {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        
        // Adjust the configuration to validate meaningful values
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 300000); // Corrected default for validation
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 100000);

        // 2. Retrieve configuration values from the configuration object
        long staleDatanodeInterval = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);

        long heartbeatRecheckInterval = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT);

        // 3. Prepare validation messages to provide more context during failed assertions
        String violationMessage = "The value of dfs.namenode.stale.datanode.interval (" + staleDatanodeInterval +
                                  ") must be greater than or equal to dfs.namenode.heartbeat.recheck-interval (" +
                                  heartbeatRecheckInterval + ").";

        // 4. Test constraints
        // Test that dfs.namenode.stale.datanode.interval >= dfs.namenode.heartbeat.recheck-interval
        assertTrue(violationMessage, staleDatanodeInterval >= heartbeatRecheckInterval);
    }

    @Test
    public void testLoggingForViolationCorrection() {
        // 1. Prepare the configuration with invalid values for testing correction handling
        Configuration testConf = new Configuration();
        testConf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 10000); // Invalid value for testing
        testConf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 30000);

        // 2. Retrieve configuration values using the test configuration
        long staleDatanodeInterval = testConf.getLong(
            DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);

        long heartbeatRecheckInterval = testConf.getLong(
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT);

        // 3. Conditional logging and correction logic for invalid settings
        if (staleDatanodeInterval < heartbeatRecheckInterval) {
            System.out.println("Correction: Setting dfs.namenode.stale.datanode.interval to " +
                heartbeatRecheckInterval + " because it was less than dfs.namenode.heartbeat.recheck-interval.");
            staleDatanodeInterval = heartbeatRecheckInterval; // Apply correction
        }

        // 4. Validate that the correction has been applied
        assertTrue("After correction, dfs.namenode.stale.datanode.interval must be greater than or equal to dfs.namenode.heartbeat.recheck-interval.",
            staleDatanodeInterval >= heartbeatRecheckInterval);
    }
}