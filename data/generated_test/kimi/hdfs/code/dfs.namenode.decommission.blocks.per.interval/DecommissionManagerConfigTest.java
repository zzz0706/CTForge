package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DecommissionManagerConfigTest {

    @Mock
    private ScheduledExecutorService mockExecutor;

    @Mock
    private Namesystem mockNamesystem;

    @Mock
    private BlockManager mockBlockManager;

    @Mock
    private HeartbeatManager mockHeartbeatManager;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDecommissionBlocksPerInterval_ConfigValueIsUsedInMonitor() throws IOException {
        // Load default configuration from file to avoid hardcoding
        Configuration conf = new Configuration();
        Properties defaultProps = new Properties();
        
        // Get expected value from configuration service (via Configuration class)
        String key = DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY;
        int expectedBlocksPerInterval = conf.getInt(key, DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

        // Prepare DecommissionManager with mocked dependencies
        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Use reflection to set the executor
        setExecutorField(manager, mockExecutor);

        // Activate decommission manager
        manager.activate(conf);

        // Capture scheduled task arguments
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Long> intervalCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<TimeUnit> timeUnitCaptor = ArgumentCaptor.forClass(TimeUnit.class);

        verify(mockExecutor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                intervalCaptor.capture(),
                intervalCaptor.capture(), // same interval for initial delay and period
                timeUnitCaptor.capture()
        );

        // Verify that Monitor was created with correct blocksPerInterval
        Runnable monitor = runnableCaptor.getValue();
        assertEquals("Monitor should be initialized with correct numBlocksPerCheck",
                expectedBlocksPerInterval, getField(monitor, "numBlocksPerCheck"));
    }

    @Test
    public void testDecommissionBlocksPerInterval_OverrideViaDeprecatedKey() throws IOException {
        // Setup configuration with deprecated key
        Configuration conf = new Configuration();
        conf.set("dfs.namenode.decommission.nodes.per.interval", "10");

        // Get expected behavior: deprecated key should override default for blocks per interval
        int expectedNodesPerInterval = 10;
        int expectedBlocksPerInterval = Integer.MAX_VALUE; // As per logic when deprecated key is used

        // Prepare DecommissionManager
        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Use reflection to set the executor
        setExecutorField(manager, mockExecutor);

        // Activate decommission manager
        manager.activate(conf);

        // Capture scheduled task
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                anyLong(),
                anyLong(),
                any(TimeUnit.class)
        );

        // Verify Monitor initialization values
        Runnable monitor = runnableCaptor.getValue();
        assertEquals("When deprecated key is used, blocksPerInterval should be MAX_VALUE",
                expectedBlocksPerInterval, getField(monitor, "numBlocksPerCheck"));
        assertEquals("When deprecated key is used, nodesPerInterval should take its value",
                expectedNodesPerInterval, getField(monitor, "numNodesPerCheck"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecommissionBlocksPerInterval_NegativeValueThrows() {
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, -1);

        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Use reflection to set the executor
        setExecutorField(manager, mockExecutor);

        manager.activate(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecommissionBlocksPerInterval_ZeroValueThrows() {
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, 0);

        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Use reflection to set the executor
        setExecutorField(manager, mockExecutor);

        manager.activate(conf);
    }

    @Test
    public void testDecommissionBlocksPerInterval_PositiveValueAccepted() {
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, 1000);

        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        
        // Use reflection to set the executor
        setExecutorField(manager, mockExecutor);

        // Should not throw exception
        manager.activate(conf);
        verify(mockExecutor).scheduleAtFixedRate(
                any(Runnable.class),
                anyLong(),
                anyLong(),
                any(TimeUnit.class)
        );
    }

    // Helper method to access private fields for verification
    private int getField(Object monitor, String fieldName) {
        try {
            java.lang.reflect.Field field = monitor.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (Integer) field.get(monitor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    // Helper method to set private executor field
    private void setExecutorField(DecommissionManager manager, ScheduledExecutorService executor) {
        try {
            java.lang.reflect.Field field = DecommissionManager.class.getDeclaredField("executor");
            field.setAccessible(true);
            field.set(manager, executor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}