package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import java.net.ServerSocket;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LeaderInitializationTest {
    
    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    // Test code:
    // 1. Correctly load the configuration values using the zookeeper3.5.6 API instead of hard-coded values.
    // 2. Prepare the test conditions by mocking the required objects and loading configurations as described.
    // 3. Test the behavior to ensure portUnification is disabled.
    // 4. Perform cleanup after testing.
    public void test_leader_initialization_with_port_unification_disabled() throws Exception {
        // Step 1: Load configuration values
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }
        
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 2: Prepare test mocks
        QuorumPeer quorumPeerMock = mock(QuorumPeer.class);
        LeaderZooKeeperServer zkServerMock = mock(LeaderZooKeeperServer.class);

        // Mock configuration values for portUnification and related quorum behaviors
        when(quorumPeerMock.shouldUsePortUnification()).thenReturn(false);
        when(quorumPeerMock.getQuorumListenOnAllIPs()).thenReturn(false);
        when(quorumPeerMock.getQuorumAddress()).thenReturn(config.getClientPortAddress());

        // Step 3: Initialize Leader and test behavior
        Leader leader = new Leader(quorumPeerMock, zkServerMock);

        // Extract and verify the initialized ServerSocket using reflection
        ServerSocket serverSocket = null;
        try {
            java.lang.reflect.Field ssField = Leader.class.getDeclaredField("ss");
            ssField.setAccessible(true);  // Allows access to private field
            serverSocket = (ServerSocket) ssField.get(leader);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Failed to access private field: " + e.getMessage());
        }

        // Assert behavior of portUnification configuration
        assertNotNull("ServerSocket should have been initialized", serverSocket);
        assertTrue("Expected ServerSocket instance for plaintext communication", serverSocket instanceof ServerSocket);

        // Verify mocked interactions for portUnification conditions
        verify(quorumPeerMock, atLeastOnce()).shouldUsePortUnification();
        verify(quorumPeerMock, atLeastOnce()).getQuorumListenOnAllIPs();

        // Step 4: Cleanup
        if (serverSocket != null) {
            serverSocket.close();
        }
    }
}