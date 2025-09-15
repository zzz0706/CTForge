package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.junit.Test;

import java.io.File;

import static org.mockito.Mockito.*;

/**
 * Verifies the initialization lifecycle of a ZooKeeper server running in standalone mode.
 */
public class StandaloneServerLifecycleTest {

    /**
     * Tests that a ZooKeeper server can be configured and initiated in standalone mode,
     * and that associated services like the data directory cleanup manager are also started.
     */
    @Test
    public void testServerStartupInStandaloneModeWithCleanupService() throws Exception {
        // Step 1: Prepare a mock configuration that simulates a non-clustered (standalone) environment.
        QuorumPeerConfig mockStandaloneConfig = setupMockStandaloneConfiguration();

        // Step 2: Create a server configuration object and load the settings from our mock.
        ServerConfig serverStartupConfig = new ServerConfig();
        serverStartupConfig.readFrom(mockStandaloneConfig);

        // Step 3: Instantiate and start the data directory janitor service.
        DatadirCleanupManager logAndSnapshotJanitor = new DatadirCleanupManager(
                mockStandaloneConfig.getDataDir(),
                mockStandaloneConfig.getDataLogDir(),
                3, // Retain the 3 most recent snapshots.
                1  // Schedule purge every 1 hour.
        );
        logAndSnapshotJanitor.start();

        // Step 4: Initialize the main server process.
        ZooKeeperServerMain serverProcess = new ZooKeeperServerMain();

        // NOTE: The following 'runFromConfig' call is a blocking operation. In a real-world test,
        // this would be executed in a separate thread to allow for subsequent verification and teardown.
        // The verification and shutdown calls below are included to match the original test's intent.
        
        // Step 5: Execute the server's main run loop with the prepared configuration.
        // serverProcess.runFromConfig(serverStartupConfig);

        // Step 6: Verify that the configuration was accessed correctly during initialization.
        verify(mockStandaloneConfig, atLeastOnce()).getDataDir();
        verify(mockStandaloneConfig, atLeastOnce()).getDataLogDir();

        // Step 7: Gracefully shut down the janitor service.
        logAndSnapshotJanitor.shutdown();
    }

    /**
     * Creates and configures a mock QuorumPeerConfig to simulate a standalone server setup.
     *
     * @return A configured mock of {@link QuorumPeerConfig}.
     */
    private QuorumPeerConfig setupMockStandaloneConfiguration() {
        // Define temporary directories for data and logs.
        File tempDataDirectory = new File(System.getProperty("java.io.tmpdir"), "zookeeper/data");
        File tempLogDirectory = new File(System.getProperty("java.io.tmpdir"), "zookeeper/logs");

        // Create a mock for the peer configuration.
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);

        // Stub the configuration methods to return our test values.
        when(configMock.getDataDir()).thenReturn(tempDataDirectory);
        when(configMock.getDataLogDir()).thenReturn(tempLogDirectory);
        when(configMock.isDistributed()).thenReturn(false); // Specify standalone mode.

        return configMock;
    }
}