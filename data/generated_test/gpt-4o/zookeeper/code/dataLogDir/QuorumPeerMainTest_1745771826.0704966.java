package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.DatadirCleanupManager;
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
        QuorumPeerConfig quorumPeerConfigMock = mock(QuorumPeerConfig.class);
        File dataDir = new File(System.getProperty("java.io.tmpdir") + "/zookeeper/datadir");
        File dataLogDir = new File(System.getProperty("java.io.tmpdir") + "/zookeeper/datalogdir");

        // Configure mocks for dynamic valid configuration values
        when(quorumPeerConfigMock.getDataDir()).thenReturn(dataDir);
        when(quorumPeerConfigMock.getDataLogDir()).thenReturn(dataLogDir);
        when(quorumPeerConfigMock.isDistributed()).thenReturn(false); // Single server mode

        // Step 2: Prepare test conditions
        ServerConfig serverConfigMock = new ServerConfig();
        serverConfigMock.readFrom(quorumPeerConfigMock);

        ZooKeeperServerMain zooKeeperServerMain = spy(new ZooKeeperServerMain()); // Spy ZooKeeperServerMain instance

        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(
            quorumPeerConfigMock.getDataDir(),
            quorumPeerConfigMock.getDataLogDir(),
            3, // Keep 3 snapshots
            1 // Cleanup interval of 1 hour
        );

        // Step 3: Test code execution
        try {
            cleanupManager.start(); // Ensure DatadirCleanupManager starts purge tasks
            zooKeeperServerMain.runFromConfig(serverConfigMock); // Run ZooKeeper in standalone mode
            
            // Verify methods are invoked as expected
            verify(quorumPeerConfigMock, atLeastOnce()).getDataDir();
            verify(quorumPeerConfigMock, atLeastOnce()).getDataLogDir();
            verify(zooKeeperServerMain).runFromConfig(serverConfigMock);
        } finally {
            // Step 4: Clean up after testing
            cleanupManager.shutdown(); // Shutdown DatadirCleanupManager
        }
    }
}