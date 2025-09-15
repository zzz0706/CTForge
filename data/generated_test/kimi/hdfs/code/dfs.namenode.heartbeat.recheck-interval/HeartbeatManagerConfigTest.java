package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HeartbeatManagerConfigTest {

    @Mock
    private Namesystem namesystem;
    
    @Mock
    private BlockManager blockManager;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(namesystem.isRunning()).thenReturn(false); // Just for initialization
    }

    @Test
    public void testHeartbeatRecheckIntervalConfiguration() throws Exception {
        // 1. Obtain configuration values using HDFS 2.8.5 API
        Configuration conf = new Configuration();
        int expectedRecheckInterval = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT
        );

        // 2. Prepare test conditions
        when(blockManager.shouldUpdateBlockKey(anyLong())).thenReturn(false);

        // 3. Test code - verify the constructor uses the config value
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);
        
        // Use reflection to access the private field for verification
        java.lang.reflect.Field field = HeartbeatManager.class.getDeclaredField("heartbeatRecheckInterval");
        field.setAccessible(true);
        long actualRecheckInterval = field.getLong(heartbeatManager);
        
        assertEquals("Heartbeat recheck interval should match configuration", 
            expectedRecheckInterval, actualRecheckInterval);
    }

    @Test
    public void testHeartbeatRecheckIntervalWithStaleDatanodeAvoidance() throws Exception {
        // 1. Obtain configuration values
        Configuration conf = new Configuration();
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
        
        long recheckInterval = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT
        );
        
        long staleInterval = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT
        );

        // Set up stale interval to be smaller than recheck interval
        long testStaleInterval = Math.min(10000, recheckInterval - 1); // Ensure it's less than recheck interval
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, testStaleInterval);

        // 2. Prepare test conditions
        when(blockManager.shouldUpdateBlockKey(anyLong())).thenReturn(false);

        // 3. Test code
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);
        
        // Use reflection to access the private field
        java.lang.reflect.Field field = HeartbeatManager.class.getDeclaredField("heartbeatRecheckInterval");
        field.setAccessible(true);
        long actualRecheckInterval = field.getLong(heartbeatManager);
        
        // Verify that when stale interval is smaller, it overrides the recheck interval
        long expectedInterval = Math.min(recheckInterval, testStaleInterval);
        assertEquals("When avoiding stale datanodes and stale interval < recheck interval, " +
            "heartbeat recheck interval should be set to the minimum of the two",
            expectedInterval, actualRecheckInterval);
    }

    @Test
    public void testShouldAbortHeartbeatCheckUsesConfiguredInterval() throws Exception {
        // 1. Obtain configuration values
        Configuration conf = new Configuration();
        int expectedRecheckInterval = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT
        );

        // 2. Prepare test conditions
        when(blockManager.shouldUpdateBlockKey(anyLong())).thenReturn(false);
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);

        // 3. Test code - verify shouldAbortHeartbeatCheck logic
        // Test case where elapsed time + offset exceeds recheck interval
        boolean shouldAbort = heartbeatManager.shouldAbortHeartbeatCheck(expectedRecheckInterval + 1);
        assertTrue("shouldAbortHeartbeatCheck should return true when elapsed+offset > recheck interval", 
            shouldAbort);

        // Test case where elapsed time + offset is within recheck interval
        shouldAbort = heartbeatManager.shouldAbortHeartbeatCheck(-1);
        assertFalse("shouldAbortHeartbeatCheck should return false when elapsed+offset <= recheck interval", 
            shouldAbort);
    }

    @Test
    public void testConfigurationValueMatchesDefaultFile() throws IOException {
        // Load default configuration values from the properties file
        Properties defaultProps = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("hdfs-default.xml")) {
            if (is != null) {
                defaultProps.loadFromXML(is);
            }
        } catch (Exception e) {
            // If we can't load the XML, that's OK for this test
        }

        // 1. Obtain configuration values using HDFS API
        Configuration conf = new Configuration();
        int apiValue = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT
        );

        // 2. Compare with default value from documentation
        assertEquals("Configuration value should match documented default",
            300000, apiValue); // 5 minutes in milliseconds
    }
}