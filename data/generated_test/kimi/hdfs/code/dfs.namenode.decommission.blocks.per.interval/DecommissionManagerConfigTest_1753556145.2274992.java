package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

import static org.junit.Assert.*;

public class DecommissionManagerConfigTest {

    private Configuration conf;
    private Namesystem mockNamesystem;
    private BlockManager mockBlockManager;
    private HeartbeatManager mockHeartbeatManager;

    @Before
    public void setUp() {
        conf = new Configuration();
        mockNamesystem = Mockito.mock(Namesystem.class);
        mockBlockManager = Mockito.mock(BlockManager.class);
        mockHeartbeatManager = Mockito.mock(HeartbeatManager.class);
        Mockito.when(mockNamesystem.isRunning()).thenReturn(true);
    }

    @Test
    // Verify that setting dfs.namenode.decommission.blocks.per.interval to zero results in an IllegalArgumentException.
    // 1. Use DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY to set the configuration value.
    // 2. Prepare test condition: Set the configuration key to 0.
    // 3. Create DecommissionManager instance and call activate() with the Configuration.
    // 4. Assert that an IllegalArgumentException is thrown with a message indicating the value must be positive.
    public void testDecommissionBlocksPerInterval_ZeroValueThrowsException() throws Exception {
        // Prepare test condition: Set the configuration key to 0
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, 0);
        
        // Create DecommissionManager instance with proper constructor parameters
        DecommissionManager decommissionManager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Test and assert: Wrap the activate() call in a try-catch block to capture exceptions
        try {
            decommissionManager.activate(conf);
            fail("Expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            // Assert that an IllegalArgumentException is thrown with a message indicating the value must be positive
            assertTrue("Exception message should indicate value must be positive: " + e.getMessage(),
                    e.getMessage().contains("Must set a positive value for") &&
                    e.getMessage().contains(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY));
        }
    }

    @Test
    // Test that the default value for dfs.namenode.decommission.blocks.per.interval is correctly used.
    // 1. Use DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT to verify the default value.
    // 2. Prepare: Do not set the config key to test default behavior.
    // 3. Create DecommissionManager instance and call activate() with the Configuration.
    // 4. Access the monitor field and verify that the default value is used correctly.
    public void testDecommissionBlocksPerIntervalDefault() throws Exception {
        // Prepare: Do not set the config key to test default behavior
        
        // Create DecommissionManager instance with proper constructor parameters
        DecommissionManager decommissionManager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Call activate to initialize the monitor
        decommissionManager.activate(conf);
        
        // Access the monitor field to verify default value usage
        Field monitorField = DecommissionManager.class.getDeclaredField("monitor");
        monitorField.setAccessible(true);
        Object monitor = monitorField.get(decommissionManager);
        
        // Access the numBlocksPerCheck field from Monitor to verify the default value
        Field numBlocksPerCheckField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        numBlocksPerCheckField.setAccessible(true);
        int blocksPerInterval = numBlocksPerCheckField.getInt(monitor);
        
        // Verify that the default value is used correctly
        assertEquals(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT, blocksPerInterval);
    }

    @Test
    // Test that a custom value for dfs.namenode.decommission.blocks.per.interval is correctly used.
    // 1. Use DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY to set a custom value.
    // 2. Prepare test condition: Set a custom value for the configuration key.
    // 3. Create DecommissionManager instance and call activate() with the Configuration.
    // 4. Access the monitor field and verify that the custom value is used correctly.
    public void testDecommissionBlocksPerIntervalCustomValue() throws Exception {
        // Prepare test condition
        int customValue = 100000;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, customValue);

        // Create DecommissionManager instance with proper constructor parameters
        DecommissionManager decommissionManager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Call activate to initialize the monitor
        decommissionManager.activate(conf);
        
        // Access the monitor field to verify custom value usage
        Field monitorField = DecommissionManager.class.getDeclaredField("monitor");
        monitorField.setAccessible(true);
        Object monitor = monitorField.get(decommissionManager);
        
        // Access the numBlocksPerCheck field from Monitor to verify the custom value
        Field numBlocksPerCheckField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        numBlocksPerCheckField.setAccessible(true);
        int blocksPerInterval = numBlocksPerCheckField.getInt(monitor);
        
        // Test and assert
        assertEquals(customValue, blocksPerInterval);
    }

    @Test
    // Test that the deprecated configuration key dfs.namenode.decommission.nodes.per.interval is correctly handled.
    // 1. Use the deprecated key "dfs.namenode.decommission.nodes.per.interval" to set a value.
    // 2. Prepare test condition: Set the deprecated configuration key to a value and do not set the new key.
    // 3. Create DecommissionManager instance and call activate() with the Configuration.
    // 4. Access the monitor field and verify that the deprecated key takes effect correctly.
    public void testDecommissionBlocksPerIntervalWithDeprecatedKey() throws Exception {
        // Prepare test condition
        int deprecatedValue = 5;
        conf.setInt("dfs.namenode.decommission.nodes.per.interval", deprecatedValue);
        // Do not set the new key to ensure deprecated key takes effect

        // Create DecommissionManager instance with proper constructor parameters
        DecommissionManager decommissionManager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Call activate to initialize the monitor
        decommissionManager.activate(conf);
        
        // Access the monitor field to verify deprecated key usage
        Field monitorField = DecommissionManager.class.getDeclaredField("monitor");
        monitorField.setAccessible(true);
        Object monitor = monitorField.get(decommissionManager);
        
        // Access the numBlocksPerCheck and numNodesPerCheck fields from Monitor
        Field numBlocksPerCheckField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        numBlocksPerCheckField.setAccessible(true);
        int blocksPerInterval = numBlocksPerCheckField.getInt(monitor);
        
        Field numNodesPerCheckField = monitor.getClass().getDeclaredField("numNodesPerCheck");
        numNodesPerCheckField.setAccessible(true);
        int nodesPerInterval = numNodesPerCheckField.getInt(monitor);

        // Test and assert
        assertEquals(Integer.MAX_VALUE, blocksPerInterval);
        assertEquals(deprecatedValue, nodesPerInterval);
    }
}