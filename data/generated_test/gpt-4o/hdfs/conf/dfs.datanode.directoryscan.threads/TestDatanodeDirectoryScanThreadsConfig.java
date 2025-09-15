package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestDatanodeDirectoryScanThreadsConfig {
    
    /**
     * Test to validate the configuration value of `dfs.datanode.directoryscan.threads`.
     */
    @Test
    public void testDirectoryScanThreadsConfigValidation() {
        // Step 1: Set up a configuration object
        Configuration conf = new Configuration();

        // Step 2: Read the configuration value from the file
        int directoryScanThreads = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT
        );

        // Step 3: Validate the value based on the constraints
        // Constraint: The value must be a positive integer (>=1), as it represents the number of threads.
        Assert.assertTrue(
            "The configuration value for " + DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY +
            " must be a positive integer (>= 1). Current value: " + directoryScanThreads,
            directoryScanThreads >= 1
        );
    }
}