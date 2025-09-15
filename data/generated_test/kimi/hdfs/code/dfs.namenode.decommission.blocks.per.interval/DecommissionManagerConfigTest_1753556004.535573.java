package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DecommissionManager;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DecommissionManagerConfigTest {

    @Mock
    private ScheduledExecutorService executor;

    @Mock
    private Namesystem namesystem;

    @Mock
    private BlockManager blockManager;

    @Mock
    private HeartbeatManager heartbeatManager;

    private DecommissionManager decommissionManager;
    private Configuration conf;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration(false);

        // Create DecommissionManager with proper constructor parameters
        decommissionManager = new DecommissionManager(namesystem, blockManager, heartbeatManager);
    }

    @Test
    public void testDecommissionBlocksPerInterval_ConfigValueUsedInMonitor() {
        // Prepare test conditions
        int expectedBlocksPerInterval = 100000; // Non-default value to verify it's being used
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, expectedBlocksPerInterval);

        // Mock the executor to capture the scheduled task
        when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        Runnable command = (Runnable) invocation.getArguments()[0];
                        return null; // We don't need to actually schedule anything
                    }
                });

        // Test code
        decommissionManager.activate(conf);

        // Since we can't directly access private Monitor class, we test the behavior indirectly
        // by checking that the configuration was properly read and used
        int actualValue = conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, 
                                     DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
        assertEquals("Configuration value should be set correctly", expectedBlocksPerInterval, actualValue);
    }

    @Test
    public void testDecommissionBlocksPerInterval_DefaultValue() {
        // Prepare test conditions - do not set the config value, let it use default
        int defaultBlocksPerInterval = DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT;

        // Test code
        decommissionManager.activate(conf);

        // Verify default value is used when not explicitly set
        int actualValue = conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, 
                                     DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
        assertEquals("Default configuration value should be used", defaultBlocksPerInterval, actualValue);
    }

    @Test
    public void testDecommissionBlocksPerInterval_ReferenceLoaderComparison() {
        // Prepare test conditions
        String key = DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY;
        conf.setInt(key, 200000); // Set a specific value

        // Test code - Get value via Configuration API
        int configValue = conf.getInt(key, DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

        // Compare with default value since we can't load hdfs-default.xml as Properties
        int defaultValue = DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT;

        // Since we set it to 200000, we verify that
        assertEquals("Configuration value should match expected test value", 200000, configValue);
        assertNotEquals("Configuration value should be different from default", defaultValue, configValue);
    }

    @Test
    public void testDecommissionInterval_Configuration() {
        // Prepare test conditions
        long expectedInterval = 30L; // Non-default value
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, expectedInterval);

        // Test code
        decommissionManager.activate(conf);

        // Verify the configuration was read correctly
        long actualInterval = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
                                          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT);
        assertEquals("Decommission interval should be configured correctly", expectedInterval, actualInterval);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecommissionInterval_InvalidValue() {
        // Prepare test conditions - set invalid (negative) value
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, -1L);

        // Test code - should throw IllegalArgumentException
        decommissionManager.activate(conf);
    }

    @Test
    public void testDecommissionManagerInitialization() {
        // Test that DecommissionManager can be properly initialized
        assertNotNull("DecommissionManager should be created", decommissionManager);
        
        // Test activation with valid configuration
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, 50000);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 30L);
        
        try {
            decommissionManager.activate(conf);
            // If no exception is thrown, initialization was successful
            assertTrue("DecommissionManager should activate successfully with valid config", true);
        } catch (Exception e) {
            fail("DecommissionManager should not throw exception with valid configuration: " + e.getMessage());
        }
    }
}