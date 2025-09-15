package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class DecommissionManagerConfigTest {

    private Configuration conf;
    private DecommissionManager decommissionManager;
    private BlockManager blockManager;

    @Mock
    private FSNamesystem namesystem;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
        blockManager = mock(BlockManager.class);
        // Need to provide all 3 required parameters: Namesystem, BlockManager, HeartbeatManager
        decommissionManager = new DecommissionManager(namesystem, blockManager, mock(HeartbeatManager.class));
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDecommissionBlocksPerInterval_ConfigValueUsedInMonitor() throws Exception {
        // 1. Obtain configuration value using HDFS 2.8.5 API (no hardcoding)
        int defaultBlocksPerInterval = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT
        );
        
        // Use a non-default value for testing
        int expectedBlocksPerInterval = defaultBlocksPerInterval + 100;

        // 2. Prepare test conditions: set configuration
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, expectedBlocksPerInterval);

        // 3. Invoke the method under test
        decommissionManager.activate(conf);

        // 4. Verify that Monitor was created with the correct numBlocksPerCheck
        Field monitorField = DecommissionManager.class.getDeclaredField("monitor");
        monitorField.setAccessible(true);
        Object monitor = monitorField.get(decommissionManager);
        
        Field blocksPerIntervalField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        blocksPerIntervalField.setAccessible(true);
        int actualBlocksPerInterval = (Integer) blocksPerIntervalField.get(monitor);
        
        assertEquals(expectedBlocksPerInterval, actualBlocksPerInterval);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDecommissionBlocksPerInterval_WithDeprecatedKey() throws Exception {
        // Set deprecated key
        final String deprecatedKey = "dfs.namenode.decommission.nodes.per.interval";
        int deprecatedValue = 10;
        conf.set(deprecatedKey, String.valueOf(deprecatedValue));

        // 1. Get expected values
        int expectedNodesPerInterval = conf.getInt(deprecatedKey, Integer.MAX_VALUE);
        // When deprecated key is set, blocks per interval should default to MAX_VALUE

        // 2. Prepare test conditions and execute
        decommissionManager.activate(conf);

        // 3. Verify
        Field monitorField = DecommissionManager.class.getDeclaredField("monitor");
        monitorField.setAccessible(true);
        Object monitor = monitorField.get(decommissionManager);
        
        Field blocksPerIntervalField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        blocksPerIntervalField.setAccessible(true);
        int actualBlocksPerInterval = (Integer) blocksPerIntervalField.get(monitor);
        
        Field nodesPerIntervalField = monitor.getClass().getDeclaredField("numNodesPerCheck");
        nodesPerIntervalField.setAccessible(true);
        int actualNodesPerInterval = (Integer) nodesPerIntervalField.get(monitor);
        
        assertEquals(Integer.MAX_VALUE, actualBlocksPerInterval);
        assertEquals(expectedNodesPerInterval, actualNodesPerInterval);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDecommissionBlocksPerInterval_DefaultValue() throws Exception {
        // Ensure no custom value is set
        conf.unset(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY);
        conf.unset("dfs.namenode.decommission.nodes.per.interval");

        // 1. Get expected value from API
        int expectedBlocksPerInterval = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT
        );

        // 2. Execute
        decommissionManager.activate(conf);

        // 3. Verify
        Field monitorField = DecommissionManager.class.getDeclaredField("monitor");
        monitorField.setAccessible(true);
        Object monitor = monitorField.get(decommissionManager);
        
        Field blocksPerIntervalField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        blocksPerIntervalField.setAccessible(true);
        int actualBlocksPerInterval = (Integer) blocksPerIntervalField.get(monitor);
        
        assertEquals(expectedBlocksPerInterval, actualBlocksPerInterval);
    }
    
    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDecommissionBlocksPerInterval_NegativeValueThrowsException() {
        // Create a Configuration object and set dfs.namenode.decommission.blocks.per.interval to -100
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, -100);
        
        try {
            // Instantiate DecommissionManager and attempt to call activate() with the Configuration
            decommissionManager.activate(conf);
            fail("Expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            // Assert that an IllegalArgumentException is thrown with a message indicating the value must be positive
            assertEquals("Must set a positive value for " + 
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, e.getMessage());
        }
    }
}