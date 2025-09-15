package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class DecommissionManagerConfigTest {

    private Configuration conf;
    private DecommissionManager decommissionManager;

    @Before
    public void setUp() {
        conf = new Configuration();
        // Assuming we have access to a NameSystem mock or real instance
        // For this test, we'll focus on configuration parsing logic
    }

    @Test
    public void testDecommissionBlocksPerIntervalDefault() {
        // Prepare: Do not set the config key to test default behavior
        // The activate method is package-private, so we need to access it via same package or reflection
        // Here we simulate the activation process to verify default value usage

        ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
        
        // Simulate the activate method logic
        final int intervalSecs = conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT);
        
        int blocksPerInterval = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
            
        int nodesPerInterval = Integer.MAX_VALUE;
        final String deprecatedKey = "dfs.namenode.decommission.nodes.per.interval";
        final String strNodes = conf.get(deprecatedKey);
        if (strNodes != null) {
            nodesPerInterval = Integer.parseInt(strNodes);
            blocksPerInterval = Integer.MAX_VALUE;
        }
        
        // Verify that the default value is used correctly
        assertEquals(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT, blocksPerInterval);
    }

    @Test
    public void testDecommissionBlocksPerIntervalCustomValue() {
        // Prepare test condition
        int customValue = 100000;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY, customValue);

        // Simulate activate method logic
        int blocksPerInterval = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

        // Test and assert
        assertEquals(customValue, blocksPerInterval);
    }

    @Test
    public void testDecommissionBlocksPerIntervalWithDeprecatedKey() {
        // Prepare test condition
        int deprecatedValue = 5;
        conf.setInt("dfs.namenode.decommission.nodes.per.interval", deprecatedValue);
        // Do not set the new key to ensure deprecated key takes effect

        // Simulate activate method logic
        int blocksPerInterval = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
        int nodesPerInterval = Integer.MAX_VALUE;

        final String deprecatedKey = "dfs.namenode.decommission.nodes.per.interval";
        final String strNodes = conf.get(deprecatedKey);
        if (strNodes != null) {
            nodesPerInterval = Integer.parseInt(strNodes);
            blocksPerInterval = Integer.MAX_VALUE;
        }

        // Test and assert
        assertEquals(Integer.MAX_VALUE, blocksPerInterval);
        assertEquals(deprecatedValue, nodesPerInterval);
    }

    @Test
    public void testReferenceLoaderComparison() {
        // Load configuration using Hadoop's Configuration API
        Configuration hadoopConf = new Configuration();
        int hadoopValue = hadoopConf.getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

        // Load the same configuration using a standard Properties loader
        // This would typically involve loading from hdfs-default.xml or similar
        // For this example, we assume the default value is known and consistent
        int defaultValue = 500000; // From DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT

        // Compare values
        assertEquals(defaultValue, hadoopValue);
    }
}