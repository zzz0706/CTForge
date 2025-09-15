package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
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
    public void testDecommissionBlocksPerInterval_DefaultValueUsedWhenNotSet() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        // Ensure neither the main key nor the deprecated key is set
        conf.unset(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY);
        conf.unset("dfs.namenode.decommission.nodes.per.interval");

        // Get expected default value from configuration service
        int expectedBlocksPerInterval = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

        // 2. Prepare the test conditions.
        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        setExecutorField(manager, mockExecutor);

        // 3. Test code.
        manager.activate(conf);

        // Capture scheduled task arguments
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                anyLong(),
                anyLong(),
                any(TimeUnit.class)
        );

        // 4. Code after testing.
        // Verify that Monitor was created with correct blocksPerInterval (default value)
        Runnable monitor = runnableCaptor.getValue();
        assertEquals("Monitor should be initialized with default numBlocksPerCheck",
                expectedBlocksPerInterval, getField(monitor, "numBlocksPerCheck"));
    }

    @Test
    public void testDecommissionBlocksPerInterval_ConfigValueIsUsedInMonitor() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        int customBlocksPerInterval = 100000;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, customBlocksPerInterval);

        // 2. Prepare the test conditions.
        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        setExecutorField(manager, mockExecutor);

        // 3. Test code.
        manager.activate(conf);

        // Capture scheduled task arguments
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                anyLong(),
                anyLong(),
                any(TimeUnit.class)
        );

        // 4. Code after testing.
        // Verify that Monitor was created with correct blocksPerInterval
        Runnable monitor = runnableCaptor.getValue();
        assertEquals("Monitor should be initialized with configured numBlocksPerCheck",
                customBlocksPerInterval, getField(monitor, "numBlocksPerCheck"));
    }

    @Test
    public void testDecommissionBlocksPerInterval_OverrideViaDeprecatedKey() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        int deprecatedNodesPerInterval = 10;
        conf.set("dfs.namenode.decommission.nodes.per.interval", String.valueOf(deprecatedNodesPerInterval));
        // Ensure main key is not set
        conf.unset(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY);

        // 2. Prepare the test conditions.
        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        setExecutorField(manager, mockExecutor);

        // 3. Test code.
        manager.activate(conf);

        // Capture scheduled task
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutor).scheduleAtFixedRate(
                runnableCaptor.capture(),
                anyLong(),
                anyLong(),
                any(TimeUnit.class)
        );

        // 4. Code after testing.
        // Verify Monitor initialization values
        Runnable monitor = runnableCaptor.getValue();
        assertEquals("When deprecated key is used, blocksPerInterval should be MAX_VALUE",
                Integer.MAX_VALUE, getField(monitor, "numBlocksPerCheck"));
        assertEquals("When deprecated key is used, nodesPerInterval should take its value",
                deprecatedNodesPerInterval, getField(monitor, "numNodesPerCheck"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecommissionBlocksPerInterval_NegativeValueThrows() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, -1);

        // 2. Prepare the test conditions.
        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        setExecutorField(manager, mockExecutor);

        // 3. Test code.
        // Should throw IllegalArgumentException
        manager.activate(conf);
        
        // 4. Code after testing.
        // Exception expected, no further assertions needed
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecommissionBlocksPerInterval_ZeroValueThrows() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, 0);

        // 2. Prepare the test conditions.
        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        setExecutorField(manager, mockExecutor);

        // 3. Test code.
        // Should throw IllegalArgumentException
        manager.activate(conf);
        
        // 4. Code after testing.
        // Exception expected, no further assertions needed
    }

    @Test
    public void testDecommissionBlocksPerInterval_PositiveValueAccepted() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        int positiveBlocksPerInterval = 1000;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, positiveBlocksPerInterval);

        // 2. Prepare the test conditions.
        DecommissionManager manager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
        setExecutorField(manager, mockExecutor);

        // 3. Test code.
        // Should not throw exception
        manager.activate(conf);
        
        // 4. Code after testing.
        verify(mockExecutor).scheduleAtFixedRate(
                any(Runnable.class),
                anyLong(),
                anyLong(),
                any(TimeUnit.class)
        );
    }

    // Helper method to access private fields for verification
    private int getField(Object monitor, String fieldName) throws Exception {
        Class<?> clazz = monitor.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (Integer) field.get(monitor);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("Field '" + fieldName + "' not found in class hierarchy");
    }
    
    // Helper method to set private executor field
    private void setExecutorField(DecommissionManager manager, ScheduledExecutorService executor) throws Exception {
        Field field = DecommissionManager.class.getDeclaredField("executor");
        field.setAccessible(true);
        field.set(manager, executor);
    }
}