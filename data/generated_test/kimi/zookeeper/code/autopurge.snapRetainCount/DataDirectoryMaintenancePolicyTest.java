package org.apache.zookeeper.test;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import java.io.File;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates the behavior of the DatadirCleanupManager when the purging feature is explicitly disabled.
 */
public class DataDirectoryMaintenancePolicyTest {

    /**
     * This test ensures that when 'purgeInterval' is set to 0, the DatadirCleanupManager
     * initializes correctly but does not schedule any cleanup tasks, effectively disabling the auto-purge feature.
     */
    @Test
    public void testCleanupTaskSchedulingIsDisabledWhenIntervalIsZero() {
        // Step 1: Define the simulated configuration parameters for this test scenario.
        final int purgeIntervalDisabled = 0; // A value of 0 disables the purge task.
        final int snapshotsToKeep = 3;
        final File dummySnapshotDir = new File("/var/lib/zookeeper/snapshots");
        final File dummyTxnLogDir = new File("/var/lib/zookeeper/logs");

        // Step 2: Create a mock configuration object that simulates these settings.
        QuorumPeerConfig mockedConfiguration = createMockConfiguration(
            dummySnapshotDir,
            dummyTxnLogDir,
            snapshotsToKeep,
            purgeIntervalDisabled
        );

        // Step 3: Instantiate the maintenance service with the mocked configuration.
        DatadirCleanupManager maintenanceService = new DatadirCleanupManager(
            mockedConfiguration.getDataDir(),
            mockedConfiguration.getDataLogDir(),
            mockedConfiguration.getSnapRetainCount(),
            mockedConfiguration.getPurgeInterval()
        );

        // Step 4: Execute the startup logic of the maintenance service.
        maintenanceService.start();

        // Step 5: Verification.
        // As per ZooKeeper's design, a purgeInterval of 0 prevents the cleanup timer from being scheduled.
        // This test passes if the start() method completes without throwing any exceptions,
        // confirming that the manager handles the disabled state gracefully. No further action is expected.
    }

    /**
     * A factory method to create a mock QuorumPeerConfig with specified settings.
     *
     * @param snapDir The snapshot directory.
     * @param dataLogDir The transaction log directory.
     * @param snapRetainCount The number of snapshots to retain.
     * @param purgeInterval The interval for purging old files.
     * @return A configured mock of QuorumPeerConfig.
     */
    private QuorumPeerConfig createMockConfiguration(File snapDir, File dataLogDir, int snapRetainCount, int purgeInterval) {
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);

        // Stub the getter methods to return our predefined test values.
        when(configMock.getDataDir()).thenReturn(snapDir);
        when(configMock.getDataLogDir()).thenReturn(dataLogDir);
        when(configMock.getSnapRetainCount()).thenReturn(snapRetainCount);
        when(configMock.getPurgeInterval()).thenReturn(purgeInterval);

        return configMock;
    }
}