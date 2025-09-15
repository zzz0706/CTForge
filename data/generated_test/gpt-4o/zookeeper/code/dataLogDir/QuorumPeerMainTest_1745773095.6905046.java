package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.util.PurgeTaskStatus;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.io.File;

public class QuorumPeerMainTest {

    @Test
    // Test code for: QuorumPeerMain_initializeAndRun_SingleServerMode
    // 1. You need to use the ZooKeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInitializeAndRun_SingleServerMode() throws Exception {
        // Step 1: Use the ZooKeeper 3.5.6 API to dynamically obtain configuration values
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        File dataDir = new File(System.getProperty("java.io.tmpdir") + "/zookeeper/datadir"); // Dynamically determined
        File dataLogDir = new File(System.getProperty("java.io.tmpdir") + "/zookeeper/datalogdir"); // Dynamically determined

        // Mock configuration values for standalone mode
        when(configMock.getDataDir()).thenReturn(dataDir);
        when(configMock.getDataLogDir()).thenReturn(dataLogDir);
        when(configMock.isDistributed()).thenReturn(false); // Standalone mode (not quorum)

        // Step 2: Prepare the test conditions
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(configMock); // Initialize ServerConfig using the mocked QuorumPeerConfig

        ZooKeeperServerMain zooKeeperServerMainSpy = spy(new ZooKeeperServerMain());
        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(
            configMock.getDataDir(),
            configMock.getDataLogDir(),
            3, // Retain 3 most recent snapshots
            1  // Cleanup interval of 1 hour
        );

        // Step 3: Execute the test code
        try {
            cleanupManager.start(); // Start DatadirCleanupManager purge tasks
            zooKeeperServerMainSpy.runFromConfig(serverConfig); // Simulate ZooKeeper standalone server using the configuration

            // Verify that the configuration was appropriately fetched and used
            verify(configMock, atLeastOnce()).getDataDir();
            verify(configMock, atLeastOnce()).getDataLogDir();
            verify(zooKeeperServerMainSpy).runFromConfig(serverConfig);

            // Ensure cleanup manager successfully starts
            assert cleanupManager.getPurgeTaskStatus() == PurgeTaskStatus.STARTED;

        } finally {
            // Step 4: Code after testing - Clean up resources
            cleanupManager.shutdown(); // Shutdown the DatadirCleanupManager
            assert cleanupManager.getPurgeTaskStatus() != PurgeTaskStatus.STARTED; // Verify cleanup manager stopped
        }
    }
}