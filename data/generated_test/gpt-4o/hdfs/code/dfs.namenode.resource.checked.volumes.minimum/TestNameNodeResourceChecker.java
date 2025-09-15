package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestNameNodeResourceChecker {

    @Test
    public void testNameNodeResourceChecker_ValidConfigurationParsing() throws Exception {
        // Step 1: Initialize the Configuration instance
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, "/tmp/hadoop/dfs/name");
        
        // Step 2: Prepare the necessary directories
        File nameDir = new File("/tmp/hadoop/dfs/name");
        if (!nameDir.exists()) {
            if (!nameDir.mkdirs()) {
                throw new IOException("Failed to create directory " + nameDir.getAbsolutePath());
            }
        }
        
        // Step 3: Retrieve the configuration value using the API
        int minimumVolumes = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT
        );

        // Step 4: Instantiate NameNodeResourceChecker with the configuration
        NameNodeResourceChecker resourceChecker = new NameNodeResourceChecker(conf);

        // Step 5: Simulate calling hasAvailableDiskSpace() to check the functionality
        boolean result = resourceChecker.hasAvailableDiskSpace();

        // Step 6: Assert that the functionality operates without any unexpected behavior
        assertTrue("Expected to have available disk space", result);
        
        // Cleanup: Delete created directories (optional for test isolation)
        File parentDir = new File("/tmp/hadoop/dfs/");
        if (parentDir.exists()) {
            deleteDirectory(parentDir);
        }
    }
    
    private boolean deleteDirectory(File directory) {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        return directory.delete();
    }
}