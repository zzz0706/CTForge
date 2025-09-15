package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.DecommissionManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DecommissionManagerTest {

    private Configuration mockConf;
    private DecommissionManager decommissionManager;
    private FSNamesystem mockNamesystem;
    private BlockManager mockBlockManager;
    private HeartbeatManager mockHeartbeatManager;

    @Before
    public void setUp() {
        // Mock required components
        mockConf = Mockito.mock(Configuration.class);
        mockNamesystem = Mockito.mock(FSNamesystem.class);
        mockBlockManager = Mockito.mock(BlockManager.class);
        mockHeartbeatManager = Mockito.mock(HeartbeatManager.class);

        // Initialize the DecommissionManager with mocked dependencies
        decommissionManager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testActivateWithNegativeIntervalSecs() {
        // 1. Prepare the test conditions: Set the mocked configuration value for the decommission interval.
        Mockito.when(mockConf.getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT))
            .thenReturn(-1);

        // 2. Test code: Activate the DecommissionManager with the mock configuration.
        decommissionManager.activate(mockConf);

        // 3. Expected result: IllegalArgumentException should be thrown due to negative interval seconds.
    }
}