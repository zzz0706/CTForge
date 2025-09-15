package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestConfigurationValidation {

    /**
     * Test to validate the configuration value for
     * dfs.namenode.decommission.blocks.per.interval
     */
    @Test
    public void testDecommissionBlocksPerInterval() {
        Configuration config = new Configuration();

        // Get the value of the property from the configuration.
        int blocksPerInterval = config.getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

        // Constraint 1: Must be greater than 0
        Assert.assertTrue("The configuration 'dfs.namenode.decommission.blocks.per.interval' must be greater than 0.",
                blocksPerInterval > 0);
    }

    /**
     * Test to validate dependencies between configurations.
     * E.g., ensure values for related configurations do not conflict.
     */
    @Test
    public void testConfigurationDependencies() {
        Configuration config = new Configuration();

        // Retrieve dependent configurations
        int intervalSecs = config.getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT);

        int blocksPerInterval = config.getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

        int maxConcurrentTrackedNodes = config.getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT);

        // Constraint 1: intervalSecs must be >= 0
        Assert.assertTrue("The configuration 'dfs.namenode.decommission.interval' must be >= 0.",
                intervalSecs >= 0);

        // Constraint 2: blocksPerInterval must be greater than 0 (Mapped in testDecommissionBlocksPerInterval above)
        Assert.assertTrue("The configuration 'dfs.namenode.decommission.blocks.per.interval' must be > 0.",
                blocksPerInterval > 0);

        // Constraint 3: maxConcurrentTrackedNodes must be >= 0
        Assert.assertTrue("The configuration 'dfs.namenode.decommission.max.concurrent.tracked.nodes' must be >= 0.",
                maxConcurrentTrackedNodes >= 0);
    }

    /**
     * Test to validate if deprecated configurations are handled correctly.
     */
    @Test
    public void testDeprecatedConfigHandling() {
        Configuration config = new Configuration();

        // Check if the deprecated configuration key is being used
        String deprecatedKey = "dfs.namenode.decommission.nodes.per.interval";
        String deprecatedValue = config.get(deprecatedKey);

        // If deprecated configuration is set, ensure proper fallback logic
        if (deprecatedValue != null) {
            try {
                int nodesPerInterval = Integer.parseInt(deprecatedValue);

                // Constraint: nodesPerInterval must be greater than 0
                Assert.assertTrue(
                        "The deprecated configuration 'dfs.namenode.decommission.nodes.per.interval' must be > 0.",
                        nodesPerInterval > 0);
            } catch (NumberFormatException e) {
                Assert.fail("The deprecated configuration 'dfs.namenode.decommission.nodes.per.interval' should be a valid integer.");
            }
        }
    }
}