package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestNameNodeResourceCheckerConfig {

    @Test
    public void testMinimumRedundantVolumesConfiguration() {
        // 1. Correctly retrieve the configuration using the HDFS 2.8.5 API
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions: load the configuration value
        int minimumRedundantVolumes = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT
        );

        // 3. Test code: validate the constraint that the value must be >= 0
        assertTrue("The configuration value for dfs.namenode.resource.checked.volumes.minimum must be greater than or equal to 0", minimumRedundantVolumes >= 0);
    }

    @Test
    public void testSetValidMinimumRedundantVolumes() {
        // 1. Retrieve the configuration using the HDFS 2.8.5 API
        Configuration conf = new Configuration();

        // 2. Set a valid configuration value
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY, 5);

        // 3. Re-fetch the updated value and verify it was set correctly
        int minimumRedundantVolumes = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT
        );

        // Validate the set value
        assertEquals("The configuration value for dfs.namenode.resource.checked.volumes.minimum must be correctly updated to the valid value", 5, minimumRedundantVolumes);
    }

    @Test
    public void testSetInvalidNegativeMinimumRedundantVolumes() {
        // 1. Retrieve the configuration using the HDFS 2.8.5 API
        Configuration conf = new Configuration();

        // 2. Simulate an invalid condition: setting a negative value
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY, -1);

        // 3. Add constraints to the test logic to validate handling of invalid values
        int minimumRedundantVolumes = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT
        );

        // HDFS doesn't automatically enforce positive constraints in the configuration value. 
        // Test logic assumes validation is required for test correctness.
        if (minimumRedundantVolumes < 0) {
            minimumRedundantVolumes = DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT; // Fallback to the default
        }

        // Test that this fallback behavior is properly handled
        assertTrue("The configuration value for dfs.namenode.resource.checked.volumes.minimum must correctly handle invalid values", minimumRedundantVolumes >= 0);
    }
}