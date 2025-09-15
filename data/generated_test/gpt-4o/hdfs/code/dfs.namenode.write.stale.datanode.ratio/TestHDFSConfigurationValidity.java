package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestHDFSConfigurationValidity {

    @Test
    /**
     * This test verifies the validity of the dfs.namenode.use.stale.datanode.for.write.ratio configuration value.
     */
    public void testDfsNamenodeWriteStaleDatanodeRatio() {
        // 1. Create a Configuration object to simulate a Hadoop environment
        Configuration conf = new Configuration();

        // 2. Use the Hadoop API to fetch the configuration value
        float ratioUseStaleDataNodesForWrite = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT
        );

        // 3. Check that the configuration value adheres to the constraint
        // Validity check: Must be within the range (0, 1.0]
        boolean isValid = ratioUseStaleDataNodesForWrite > 0 && ratioUseStaleDataNodesForWrite <= 1.0f;

        // 4. Assert that the value meets the expectations
        assertTrue(
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY
                        + " must be a positive float between 0 and 1.0, but got: "
                        + ratioUseStaleDataNodesForWrite,
                isValid
        );
    }

    @Test
    /**
     * This test verifies the dependency between dfs.namenode.avoid.stale.datanode.for.write
     * and dfs.namenode.use.stale.datanode.for.write.ratio.
     */
    public void testDependentConfigurationValues() {
        // 1. Create a Configuration object to simulate a Hadoop environment
        Configuration conf = new Configuration();

        // 2. Use the Hadoop API to fetch configuration values, avoiding hardcoded values
        boolean avoidWriteStaleDatanode = conf.getBoolean(
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT
        );

        float ratioUseStaleDataNodesForWrite = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT
        );

        // 3. Test the dependency condition
        // If avoidWriteStaleDatanode is enabled, the ratio must be valid
        if (avoidWriteStaleDatanode) {
            boolean isRatioValid = ratioUseStaleDataNodesForWrite > 0 && ratioUseStaleDataNodesForWrite <= 1.0f;
            assertTrue(
                    "When " + DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY
                            + " is true, " + DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY
                            + " must be a valid ratio between 0 and 1.0. Got: " + ratioUseStaleDataNodesForWrite,
                    isRatioValid
            );
        }
    }
}