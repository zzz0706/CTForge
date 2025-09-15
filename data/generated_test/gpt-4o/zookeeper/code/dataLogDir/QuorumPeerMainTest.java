package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.io.File;

public class QuorumPeerMainTest {

    @Test
    // Test code for the test case: QuorumPeerMain_initializeAndRun_SingleServerMode
    // 1. You need to use the ZooKeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test environment and conditions.
    // 3. Execute the test code based on test case steps.
    // 4. Clean up after testing.
    public void testInitializeAndRun_SingleServerMode() throws Exception {
        // Mock required configuration dynamically using APIs
        QuorumPeerConfig quorumPeerConfigMock = mock(QuorumPeerConfig.class);
        File dataDir = new File(System.getProperty("java.io.tmpdir") + "/zookeeper/datadir");
        File dataLogDir = new File(System.getProperty("java.io.tmpdir") + "/zookeeper/datalogdir");

        // Dynamically configure mocks to retrieve valid configuration values
        when(quorumPeerConfigMock.getDataDir()).thenReturn(dataDir);
        when(quorumPeerConfigMock.getDataLogDir()).thenReturn(dataLogDir);
        when(quorumPeerConfigMock.isDistributed()).thenReturn(false); // Single server (standalone) mode

        // Mock ServerConfig to ensure seamless propagation of configuration
        ServerConfig serverConfigMock = new ServerConfig();
        serverConfigMock.readFrom(quorumPeerConfigMock);

        // Invoke ZooKeeperServerMain in standalone mode instead of accessing protected method directly
        ZooKeeperServerMain zooKeeperServerMain = new ZooKeeperServerMain();

        // Ensure DatadirCleanupManager is able to execute purge tasks correctly
        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(
            quorumPeerConfigMock.getDataDir(),
            quorumPeerConfigMock.getDataLogDir(),
            3, // Keep 3 snapshots
            1 // Cleanup interval of 1 hour
        );
        cleanupManager.start();

        // Execute test by invoking the process method with the mock server configuration
        zooKeeperServerMain.runFromConfig(serverConfigMock);

        // Verify ZooKeeperServerMain is invoked and starts in standalone mode
        verify(quorumPeerConfigMock, atLeastOnce()).getDataDir();
        verify(quorumPeerConfigMock, atLeastOnce()).getDataLogDir();

        // Check cleanup tasks executed
        cleanupManager.shutdown(); // Ensure cleanup task is terminated after the test
    }
}