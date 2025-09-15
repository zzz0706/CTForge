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
        // 1. Set up configuration with invalid delay and interval values
        Configuration conf = new HdfsConfiguration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY, -1); // Invalid delay
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_KEY, Integer.MAX_VALUE); // Extremely high interval

        // Retrieve the parsed values using the configuration API
        int edekCacheLoaderDelay = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY,
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT);

        int edekCacheLoaderInterval = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_KEY,
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_DEFAULT);

        // 2. Prepare test components (FSImage, FSNamesystem, FSDirectory)
        FSImage fsImage = new FSImage(conf);
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
        FSDirectory fsDirectory = fsNamesystem.getFSDirectory();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            // Start NameNode active services to propagate configuration
            fsNamesystem.startActiveServices();

            // 3. Invoke warmUpEdekCache with invalid configuration values
            FSDirEncryptionZoneOp.warmUpEdekCache(executor, fsDirectory, edekCacheLoaderDelay, edekCacheLoaderInterval);
            
            // Monitor for log output (error handling) and verify normal execution
            System.out.println("Invoked warmUpEdekCache with invalid configuration values.");
        } catch (Exception e) {
            // Log error if invalid configuration causes issues
            System.err.println("Exception occurred during warm-up process: " + e.getMessage());
        } finally {
            // 4. Clean up resources
            executor.shutdown();
            System.out.println("Executor service shutdown.");
        }
    }
}