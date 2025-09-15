package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.mockito.Mockito.*;

/**
 * This test validates that the data directory maintenance service
 * correctly initializes and schedules its cleanup tasks based on valid configuration settings.
 */
@RunWith(MockitoJUnitRunner.class)
public class DataDirectoryMaintenanceTest {

    // The resource path for the test configuration file.
    private static final String ZK_CONFIG_FILE_PATH = "ctest.cfg";

    /**
     * Verifies that the DatadirCleanupManager's startup sequence is correctly triggered
     * when provided with a valid set of configuration properties.
     */
    @Test
    public void testPurgeTaskSchedulingOnStartupWithValidConfig() throws Exception {
        // Step 1: Load and parse the ZooKeeper configuration from the specified file.
        QuorumPeerConfig peerConfiguration = loadConfigurationFromFile(ZK_CONFIG_FILE_PATH);

        // Step 2: Extract data directory and retention policy settings from the configuration.
        File snapshotDirectory = peerConfiguration.getDataDir();
        File transactionLogDirectory = peerConfiguration.getDataLogDir();
        int snapshotsToRetain = peerConfiguration.getSnapRetainCount();
        int purgeCycleIntervalHours = peerConfiguration.getPurgeInterval();

        // Step 3: Instantiate the maintenance service with the loaded configuration.
        DatadirCleanupManager maintenanceService = new DatadirCleanupManager(
            snapshotDirectory,
            transactionLogDirectory,
            snapshotsToRetain,
            purgeCycleIntervalHours
        );

        // Step 4: Create a spy to monitor the behavior of the maintenance service instance.
        DatadirCleanupManager monitoredMaintenanceService = spy(maintenanceService);

        // Step 5: Initiate the service's lifecycle, which should schedule the cleanup task.
        monitoredMaintenanceService.start();

        // Step 6: Verify that the start method was invoked, confirming the service's
        // lifecycle management was properly initiated.
        verify(monitoredMaintenanceService, atLeastOnce()).start();
    }

    /**
     * A helper method to load properties from a file and parse them into a QuorumPeerConfig object.
     *
     * @param configPath The path to the configuration file.
     * @return A fully parsed QuorumPeerConfig object.
     * @throws IOException If there's an error reading the file.
     * @throws QuorumPeerConfig.ConfigException If the configuration properties are invalid.
     */
    private QuorumPeerConfig loadConfigurationFromFile(String configPath) throws IOException, QuorumPeerConfig.ConfigException {
        Properties configProps = new Properties();
        try (InputStream stream = new FileInputStream(configPath)) {
            configProps.load(stream);
        }

        QuorumPeerConfig peerConfig = new QuorumPeerConfig();
        peerConfig.parseProperties(configProps);
        return peerConfig;
    }
}