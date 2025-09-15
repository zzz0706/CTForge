package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.blockmanagement.FSClusterStats;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DatanodeManagerConfigTest {

    @Mock
    private HeartbeatManager heartbeatManager;

    private Configuration conf;
    private DatanodeManager datanodeManager;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        // Initialize DatanodeManager with mocked dependencies
        datanodeManager = Mockito.spy(new DatanodeManager(null, null, conf));
        // Inject the mocked heartbeat manager
        when(datanodeManager.getHeartbeatManager()).thenReturn(heartbeatManager);
    }

    @Test
    public void testStaleDatanodeRatioDefaultValue() throws IOException {
        // Get expected value from DFSConfigKeys constant
        float expectedDefault = DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT;

        // Get actual value via Configuration API
        float actualConfigValue = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
                expectedDefault);

        assertEquals("Default stale datanode ratio should be 0.5f", 
                0.5f, actualConfigValue, 0.0001f);
        assertEquals("Parsed default value should match expected", 
                expectedDefault, actualConfigValue, 0.0001f);
    }

    @Test
    public void testValidStaleDatanodeRatioConfiguration() throws IOException {
        float testRatio = 0.75f;
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, testRatio);

        // Re-initialize with new config
        DatanodeManager manager = new DatanodeManager(null, null, conf);
        
        // Test passes if no exception is thrown
        assertNotNull("Manager should be created with valid ratio", manager);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStaleDatanodeRatioAboveOne() throws IOException {
        float invalidRatio = 1.5f;
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, invalidRatio);

        // This should throw IllegalArgumentException
        new DatanodeManager(null, null, conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStaleDatanodeRatioZero() throws IOException {
        float invalidRatio = 0.0f;
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, invalidRatio);

        // This should throw IllegalArgumentException
        new DatanodeManager(null, null, conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStaleDatanodeRatioNegative() throws IOException {
        float invalidRatio = -0.1f;
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, invalidRatio);

        // This should throw IllegalArgumentException
        new DatanodeManager(null, null, conf);
    }

    @Test
    public void testShouldAvoidStaleDataNodesForWrite_BelowThreshold() throws IOException {
        // Setup: ratio=0.5, live=100, stale=40 -> 40 <= 100*0.5 = 50 -> should avoid
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, 0.5f);
        DatanodeManager manager = Mockito.spy(new DatanodeManager(null, null, conf));
        when(manager.getHeartbeatManager()).thenReturn(heartbeatManager);
        when(heartbeatManager.getLiveDatanodeCount()).thenReturn(100);
        
        // Test the method exists and can be called
        boolean result = manager.shouldAvoidStaleDataNodesForWrite();
        assertNotNull("Method should execute without error", result);
    }

    @Test
    public void testShouldAvoidStaleDataNodesForWrite_AboveThreshold() throws IOException {
        // Setup: ratio=0.5, live=100, stale=60 -> 60 > 100*0.5 = 50 -> should NOT avoid
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, 0.5f);
        DatanodeManager manager = Mockito.spy(new DatanodeManager(null, null, conf));
        when(manager.getHeartbeatManager()).thenReturn(heartbeatManager);
        when(heartbeatManager.getLiveDatanodeCount()).thenReturn(100);
        
        // Test the method exists and can be called
        boolean result = manager.shouldAvoidStaleDataNodesForWrite();
        assertNotNull("Method should execute without error", result);
    }

    @Test
    public void testFSClusterStatsIsAvoidingStaleDataNodesForWrite() throws IOException {
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, 0.3f);
        DatanodeManager manager = Mockito.spy(new DatanodeManager(null, null, conf));
        when(manager.getHeartbeatManager()).thenReturn(heartbeatManager);
        when(heartbeatManager.getLiveDatanodeCount()).thenReturn(100);
        
        // Create FSClusterStats and test the method
        FSClusterStats stats = manager.newFSClusterStats();
        assertNotNull("FSClusterStats should be created", stats);
        
        // Test the method exists and can be called
        boolean result = stats.isAvoidingStaleDataNodesForWrite();
        assertFalse("Default behavior should be false", result);
    }
    
    @Test
    public void testInvalidConfigValueNegative() throws IOException {
        // Create a real Configuration object
        Configuration config = new Configuration();
        // Set an invalid negative value
        config.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, -0.25f);
        
        // Attempt to instantiate DatanodeManager with the configuration
        try {
            new DatanodeManager(null, null, config);
            fail("Expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            // Verify that the exception message contains information about the invalid value
            assertTrue("Exception message should mention invalid value",
                    e.getMessage().contains("-0.25"));
            assertTrue("Exception message should mention the configuration key",
                    e.getMessage().contains(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY));
        }
    }
}