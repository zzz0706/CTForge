package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.FSDirEncryptionZoneOp;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestWarmUpEdekCacheInvalidConfiguration {

    @Test
    // Test warmUpEdekCache behavior when an invalid delay or interval configuration is provided.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testWarmUpEdekCacheWithInvalidConfiguration() throws IOException {
        // Step 1: Set up the configuration with invalid values
        Configuration conf = new HdfsConfiguration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY, -1); // Invalid delay configuration
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_KEY, Integer.MAX_VALUE); // Extremely high interval configuration

        int edekCacheLoaderDelay = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY,
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT);

        int edekCacheLoaderInterval = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_KEY,
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_DEFAULT);

        // Initialize FSImage and FSNamesystem
        FSImage fsImage = new FSImage(conf);
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
        FSDirectory fsDirectory = fsNamesystem.getFSDirectory(); // Obtain FSDirectory instance from FSNamesystem
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            // Step 2: Invoke FSNamesystem.startActiveServices() to propagate configuration
            fsNamesystem.startActiveServices();

            // Step 3: Invoke warmUpEdekCache with invalid configuration values
            FSDirEncryptionZoneOp.warmUpEdekCache(executor, fsDirectory, edekCacheLoaderDelay, edekCacheLoaderInterval);
        } catch (Exception e) {
            // Step 4: Log the exception if the process fails with invalid configuration
            System.err.println("Exception occurred during warm-up process: " + e.getMessage());
        } finally {
            // Step 5: Clean up resources
            executor.shutdown();
        }
    }
}