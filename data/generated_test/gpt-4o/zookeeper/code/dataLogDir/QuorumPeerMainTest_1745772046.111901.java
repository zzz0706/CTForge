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
    // Test code for the test case: QuorumPeerMain_initializeAndRun_SingleServerMode
    // 1. You need to use the ZooKeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInitializeAndRun_SingleServerMode() throws Exception {
        // Step 1: Use ZooKeeper 3.5.6 API to dynamically obtain configuration values
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        File dataDir = new File(System.getProperty("java.io.tmpdir") + "/zookeeper/datadir");
        File dataLogDir = new File(System.getProperty("java.io.tmpdir") + "/zookeeper/datalogdir");

        // Configure mocks for dynamic valid configuration values
        when(configMock.getDataDir()).thenReturn(dataDir);
        when(configMock.getDataLogDir()).thenReturn(dataLogDir);
        when(configMock.isDistributed()).thenReturn(false); // Single server mode

        // Step 2: Prepare test conditions
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(configMock);

        ZooKeeperServerMain zooKeeperServerMainSpy = spy(new ZooKeeperServerMain());
        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(
            configMock.getDataDir(),
            configMock.getDataLogDir(),
            3, // Retain 3 snapshots
            1  // Cleanup interval in hours
        );

        // Step 3: Execute the test code
        try {
            cleanupManager.start(); // Ensure DatadirCleanupManager starts purge tasks
            zooKeeperServerMainSpy.runFromConfig(serverConfig); // Run ZooKeeper in standalone mode

            // Verify the correct usage of configuration methods
            verify(configMock, atLeastOnce()).getDataDir();
            verify(configMock, atLeastOnce()).getDataLogDir();
            verify(zooKeeperServerMainSpy).runFromConfig(serverConfig);

            // Ensure cleanup manager was started successfully
            assert cleanupManager != null;
            assert cleanupManager.getPurgeTaskStatus() == PurgeTaskStatus.STARTED; 
        } finally {
            // Step 4: Clean up after testing
            cleanupManager.shutdown(); // Shutdown DatadirCleanupManager
            assert cleanupManager.getPurgeTaskStatus() != PurgeTaskStatus.STARTED;
        }
    }
}