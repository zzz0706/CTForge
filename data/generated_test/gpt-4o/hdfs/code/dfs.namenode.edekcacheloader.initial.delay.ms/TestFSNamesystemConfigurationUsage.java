package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.EncryptionZoneManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirEncryptionZoneOp;
import org.junit.Assert;
import org.junit.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestFSNamesystemConfigurationUsage {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testStartActiveServicesWithConfigurationUsage() throws Exception {
        // 1. Correctly obtain configuration values using HDFS 2.8.5 API
        Configuration conf = new Configuration();

        // Retrieve default value and define edge cases
        int defaultDelay = conf.getInt(DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY,
                DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT);
        int[] delayValues = {0, defaultDelay, 20000}; // Delay values: 0 ms, default delay, high delay

        for (int delay : delayValues) {
            // 2. Prepare the test setup with different configuration values
            conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY, delay);

            FSImage fsImage = new FSImage(conf); // Create FSImage instance
            FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage); // Initialize FSNamesystem with configuration

            FSDirectory dir = fsNamesystem.getFSDirectory();
            EncryptionZoneManager ezManager = new MockEncryptionZoneManager(dir, conf); // Mock EncryptionZoneManager

            Assert.assertNotNull("EncryptionZoneManager must be initialized", ezManager);

            // Create an executor service for EDEK cache warm-up
            ExecutorService executor = Executors.newSingleThreadExecutor();

            FSDirEncryptionZoneOp.warmUpEdekCache(executor, dir, delay, 1000); // Interval set to 1000 ms

            // 3. Validate behavior respecting configuration values
            long startTime = System.currentTimeMillis();
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            long endTime = System.currentTimeMillis();

            // Assert EDEK cache warm-up timing based on delay values
            if (delay == 0) {
                Assert.assertTrue("Warm-up task should execute immediately for delay 0",
                        (endTime - startTime) < 100);
            } else {
                Assert.assertTrue("Warm-up task respects configured delay",
                        (endTime - startTime) >= delay);
            }

            // 4. Cleanup resources post-testing
            if (!executor.isShutdown()) {
                executor.shutdownNow();
            }
        }
    }

    // Mock EncryptionZoneManager implementation for test purposes
    private static class MockEncryptionZoneManager extends EncryptionZoneManager {
        public MockEncryptionZoneManager(FSDirectory dir, Configuration conf) {
            super(dir, conf); // Constructor aligns with expected arguments
        }
        // Implement mocked methods if required
    }
}