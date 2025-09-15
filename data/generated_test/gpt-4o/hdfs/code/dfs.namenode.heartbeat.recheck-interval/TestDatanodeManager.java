package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestDatanodeManager {

    @Test
    // test case: testIsDatanodeDead_withExpiredHeartbeat
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testIsDatanodeDead_withExpiredHeartbeat() throws IOException {
        // Step 1: Configure the environment and set up the test.
        Configuration conf = new Configuration();
        long heartbeatRecheckInterval = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // Default 300000ms
        long heartbeatIntervalSeconds = conf.getLong(
                DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
                DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT); // Default 3 seconds

        long heartbeatExpireInterval = 2 * heartbeatRecheckInterval
                + 10 * 1000 * heartbeatIntervalSeconds;

        DatanodeManager datanodeManager = new DatanodeManager(null, null, conf);

        // Step 2: Create a DatanodeDescriptor and simulate the last heartbeat timestamp.
        // Correct the DatanodeID constructor to match the parameters defined in the hdfs 2.8.5 API
        DatanodeID datanodeID = new DatanodeID("127.0.0.1", "localhost", "", 1, 1, 50020, 50075);
        DatanodeDescriptor datanodeDescriptor = new DatanodeDescriptor(datanodeID);
        long lastUpdateTimestamp = Time.monotonicNow() - (heartbeatExpireInterval + 1000);
        datanodeDescriptor.setLastUpdateMonotonic(lastUpdateTimestamp);

        // Step 3: Test the isDatanodeDead() method.
        boolean isDead = datanodeManager.isDatanodeDead(datanodeDescriptor);

        // Step 4: Assert the expected result.
        assertTrue("DataNode should be considered dead due to expired heartbeat.", isDead);
    }
}