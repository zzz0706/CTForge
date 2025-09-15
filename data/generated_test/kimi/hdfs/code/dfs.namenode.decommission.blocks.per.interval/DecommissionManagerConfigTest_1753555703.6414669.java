package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DecommissionManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
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
    private Namesystem namesystem;

    @Mock
    private BlockManager blockManager;

    @Mock
    private HeartbeatManager heartbeatManager;

    @Mock
    private ScheduledExecutorService executor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDecommissionBlocksPerInterval_CustomValueUsedWhenSet() throws Exception {
        // 1. Use the HDFS 2.8.5 API to obtain configuration values
        Configuration conf = new Configuration(false);
        // Set a custom value to test non-default behavior
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, 100000);

        // 2. Prepare the test conditions
        DecommissionManager decommissionManager = new DecommissionManager(namesystem, blockManager, heartbeatManager);
        
        // Use reflection to set the executor field since it's private
        Field executorField = DecommissionManager.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        executorField.set(decommissionManager, executor);

        // 3. Test code - activate the manager to trigger config loading
        decommissionManager.activate(conf);

        // 4. Capture and verify the arguments passed to the Monitor constructor
        ArgumentCaptor<Runnable> monitorCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).scheduleAtFixedRate(monitorCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        // Access the captured Monitor instance to verify its fields
        Runnable monitor = monitorCaptor.getValue();
        
        // Assert that the numBlocksPerCheck field matches the configured value
        Field numBlocksPerCheckField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        numBlocksPerCheckField.setAccessible(true);
        int actualBlocksPerCheck = (Integer) numBlocksPerCheckField.get(monitor);
        
        assertEquals("The numBlocksPerCheck should match the configured value",
                100000, actualBlocksPerCheck);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDecommissionBlocksPerInterval_DefaultValueUsedWhenNotSet() throws Exception {
        // 1. Use the HDFS 2.8.5 API to obtain configuration values
        Configuration conf = new Configuration(false);
        // Do not set the key to test default value

        // 2. Prepare the test conditions
        DecommissionManager decommissionManager = new DecommissionManager(namesystem, blockManager, heartbeatManager);
        
        // Use reflection to set the executor field since it's private
        Field executorField = DecommissionManager.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        executorField.set(decommissionManager, executor);

        // 3. Test code - activate the manager to trigger config loading
        decommissionManager.activate(conf);

        // 4. Capture and verify the arguments passed to the Monitor constructor
        ArgumentCaptor<Runnable> monitorCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).scheduleAtFixedRate(monitorCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        // Access the captured Monitor instance to verify its fields
        Runnable monitor = monitorCaptor.getValue();
        
        // Assert that the numBlocksPerCheck field matches the default value
        Field numBlocksPerCheckField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        numBlocksPerCheckField.setAccessible(true);
        int actualBlocksPerCheck = (Integer) numBlocksPerCheckField.get(monitor);
        
        assertEquals("The numBlocksPerCheck should match the default value",
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT, actualBlocksPerCheck);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDecommissionBlocksPerInterval_DeprecatedKeyBehavior() throws Exception {
        // 1. Use the HDFS 2.8.5 API to obtain configuration values
        Configuration conf = new Configuration(false);
        // Set deprecated key
        conf.set("dfs.namenode.decommission.nodes.per.interval", "5");
        
        // 2. Prepare the test conditions
        DecommissionManager decommissionManager = new DecommissionManager(namesystem, blockManager, heartbeatManager);
        
        // Use reflection to set the executor field since it's private
        Field executorField = DecommissionManager.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        executorField.set(decommissionManager, executor);

        // 3. Test code - activate the manager to trigger config loading
        decommissionManager.activate(conf);

        // 4. Capture and verify the arguments passed to the Monitor constructor
        ArgumentCaptor<Runnable> monitorCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).scheduleAtFixedRate(monitorCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        // Access the captured Monitor instance to verify its fields
        Runnable monitor = monitorCaptor.getValue();
        
        // When deprecated key is used, blocksPerInterval should be set to Integer.MAX_VALUE
        Field numBlocksPerCheckField = monitor.getClass().getDeclaredField("numBlocksPerCheck");
        numBlocksPerCheckField.setAccessible(true);
        int actualBlocksPerCheck = (Integer) numBlocksPerCheckField.get(monitor);
        
        assertEquals("When deprecated key is used, blocksPerInterval should be MAX_VALUE",
                Integer.MAX_VALUE, actualBlocksPerCheck);
    }
}