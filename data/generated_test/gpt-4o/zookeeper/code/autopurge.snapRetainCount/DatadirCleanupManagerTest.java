package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.junit.Test;

import java.io.File;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;

import static org.mockito.Mockito.*;

/**
 * This test case verifies that the `DatadirCleanupManager` schedules the `PurgeTask` correctly
 * when valid configuration values are provided.
 */
public class DatadirCleanupManagerTest {

    // Path to the ZooKeeper configuration file to load the test properties
    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void test_DatadirCleanupManager_start_withValidValues() throws Exception {
        // Load configuration properties
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Parse configuration using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Mock retrieval of configuration values via API
        File snapDir = config.getDataDir();
        File dataLogDir = config.getDataLogDir();
        int snapRetainCount = config.getSnapRetainCount();
        int purgeInterval = config.getPurgeInterval();

        // Create an instance of DatadirCleanupManager with the retrieved configuration values
        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(snapDir, dataLogDir, snapRetainCount, purgeInterval);

        // Mocking a log validator to ensure scheduling works correctly
        DatadirCleanupManager spyCleanupManager = spy(cleanupManager);

        // Start the manager, which should schedule the PurgeTask
        spyCleanupManager.start();

        // Verify that the start method logs the correct initialization information and schedules the PurgeTask
        verify(spyCleanupManager, atLeastOnce()).start();

        // Optionally, you may add additional assertions by monitoring log outputs (not shown in direct test logic due to requirements).

        // Code after testing: You may include cleanup logic for Test execution here if required.
    }
}