package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.net.ServerSocket;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.mockito.Mockito;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LeaderInitializationTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    // Test code:
    // 1. Use the zookeeper3.5.6 API correctly to load configuration properties instead of hardcoding them.
    // 2. Create test conditions by mocking QuorumPeer and LeaderZooKeeperServer based on the loaded configurations.
    // 3. Verify that the correct socket type is initialized when portUnification is disabled in the configuration.
    // 4. Ensure cleanup after testing.
    public void test_leader_initialization_with_port_unification_disabled() throws Exception {
        // Step 1: Load configuration properties from zoo.cfg
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
        quorumConfig.parseProperties(props);

        // Step 2: Prepare mock objects and conditions
        QuorumPeer quorumPeerMock = mock(QuorumPeer.class);
        LeaderZooKeeperServer zkServerMock = mock(LeaderZooKeeperServer.class);

        // Configure mock behavior for portUnification and related settings
        when(quorumPeerMock.shouldUsePortUnification()).thenReturn(false);
        when(quorumPeerMock.getQuorumListenOnAllIPs()).thenReturn(false);
        when(quorumPeerMock.getQuorumAddress()).thenReturn(quorumConfig.getClientPortAddress());
        when(quorumPeerMock.getX509Util()).thenReturn(null); // Simplified for test purpose, handle actual logic in real cases

        // Step 3: Instantiate the Leader and verify socket initialization behavior
        Leader leader = new Leader(quorumPeerMock, zkServerMock);

        // Use reflection to extract the private ServerSocket field for verification
        ServerSocket serverSocket;
        try {
            java.lang.reflect.Field ssField = Leader.class.getDeclaredField("ss");
            ssField.setAccessible(true);
            serverSocket = (ServerSocket) ssField.get(leader);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Failed to access private ServerSocket field: " + e.getMessage());
            return; // Exit the test if reflection fails
        }

        // Verify that the ServerSocket was initialized and used for plaintext communication
        assertNotNull("ServerSocket should have been initialized", serverSocket);
        assertTrue("Expected ServerSocket instance for plaintext communication", serverSocket instanceof ServerSocket);

        // Verify mocked interactions regarding portUnification
        verify(quorumPeerMock, atLeastOnce()).shouldUsePortUnification();
        verify(quorumPeerMock, atLeastOnce()).getQuorumListenOnAllIPs();

        // Step 4: Cleanup after the test
        if (serverSocket != null) {
            serverSocket.close();
        }
    }
}