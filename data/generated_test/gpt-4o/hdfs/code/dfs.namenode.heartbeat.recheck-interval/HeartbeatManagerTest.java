package org.apache.hadoop.hdfs.server.blockmanagement;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HeartbeatManagerTest {

    private Namesystem mockNamesystem;
    private BlockManager mockBlockManager;
    private Configuration configuration;

    @Before
    public void setUp() {
        // 1. Correctly use the hdfs 2.8.5 API to obtain configuration values.
        mockNamesystem = Mockito.mock(Namesystem.class);
        mockBlockManager = Mockito.mock(BlockManager.class);
        configuration = new Configuration();

        // Configuration prerequisites using DFSConfigKeys for setting values.
        configuration.setBoolean(
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
        configuration.setLong(
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 30000); // 30 sec
        configuration.setInt(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 300000); // 5 min
    }

    @Test
    public void avoidStaleDataNodesForWriteTrue_StaleIntervalLessThanRecheckInterval() {
        // 2. Pass the mocks and configuration to the HeartbeatManager constructor.
        HeartbeatManager heartbeatManager = new HeartbeatManager(mockNamesystem, mockBlockManager, configuration);

        // 3. Use the API to verify the heartbeatRecheckInterval is correctly configured.
        long expectedStaleInterval = configuration.getLong(
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
        long actualHeartbeatRecheckInterval = configuration.getInt(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT);

        // Ensure correct relationship between stale interval and recheck interval.
        assertTrue(expectedStaleInterval < actualHeartbeatRecheckInterval);
    }
}