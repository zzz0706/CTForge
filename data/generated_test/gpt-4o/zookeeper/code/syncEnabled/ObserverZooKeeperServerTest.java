package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.quorum.ObserverZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class ObserverZooKeeperServerTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testSetupRequestProcessorsWithSyncEnabled() {
        // Test code
        // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        try {
            // Step 1: Load properties from the configuration file using ZooKeeper API
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Step 2: Parse the configuration using QuorumPeerConfig
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Mocking dependencies
            FileTxnSnapLog fileTxnSnapLogMock = Mockito.mock(FileTxnSnapLog.class);
            ZKDatabase zkDatabaseMock = Mockito.mock(ZKDatabase.class);
            
            QuorumPeer peerMock = Mockito.mock(QuorumPeer.class);
            when(peerMock.getSyncEnabled()).thenReturn(true);

            // Create the ObserverZooKeeperServer instance with mocks
            ObserverZooKeeperServer observerServer = new ObserverZooKeeperServer(
                fileTxnSnapLogMock, peerMock, zkDatabaseMock);

            // 3. Test code.
            observerServer.setupRequestProcessors();

            // Validate that syncProcessor is initialized and started
            verify(peerMock, times(1)).getSyncEnabled();

            // 4. Code after testing.
            observerServer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}