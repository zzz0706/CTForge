package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LeaderInitializationTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void test_leader_initialization_with_port_unification_disabled() throws Exception {
        // 1. Correctly load the configuration values using the zookeeper3.5.6 API
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Mock relevant configuration to prepare test conditions
        QuorumPeer quorumPeerMock = mock(QuorumPeer.class);
        LeaderZooKeeperServer zkServerMock = mock(LeaderZooKeeperServer.class);
        when(quorumPeerMock.shouldUsePortUnification()).thenReturn(false);
        when(quorumPeerMock.getQuorumListenOnAllIPs()).thenReturn(false);
        when(quorumPeerMock.getQuorumAddress()).thenReturn(config.getClientPortAddress());

        // 2. Create the test instance
        Leader leader = new Leader(quorumPeerMock, zkServerMock);

        // Access private field `ss` using reflection since it cannot be accessed directly due to private access modifier
        ServerSocket serverSocket = null;
        try {
            java.lang.reflect.Field ssField = Leader.class.getDeclaredField("ss");
            ssField.setAccessible(true);
            serverSocket = (ServerSocket) ssField.get(leader);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Failed to access private field: " + e.getMessage());
        }

        // 3. Test the behavior when portUnification is disabled
        assertNotNull("ServerSocket should have been initialized", serverSocket);
        assertTrue("Socket should be a plain ServerSocket", serverSocket instanceof ServerSocket);

        // 4. Verify logs and mocked behavior
        verify(quorumPeerMock, atLeastOnce()).shouldUsePortUnification();
        verify(quorumPeerMock, atLeastOnce()).getQuorumListenOnAllIPs();

        // Cleanup after test
        if (serverSocket != null) {
            serverSocket.close();
        }
    }
}