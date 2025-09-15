package org.apache.zookeeper.server.quorum;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Validates the socket initialization logic within the Leader class, particularly
 * how it behaves based on the 'portUnification' setting.
 */
public class LeaderSocketSetupTest {

    private static final String TEST_CONFIG_PATH = "ctest.cfg";

    /**
     * This test ensures that when port unification is disabled, the Leader initializes
     * a standard, non-SSL ServerSocket for plaintext communication.
     */
    @Test
    public void testPlainTextSocketIsUsedWhenPortUnificationIsOff() throws Exception {
        // Given: A configuration where port unification is explicitly disabled.
        QuorumPeerConfig peerConfig = loadTestConfiguration();
        QuorumPeer mockQuorumPeer = mock(QuorumPeer.class);
        LeaderZooKeeperServer mockLeaderServer = mock(LeaderZooKeeperServer.class);

        // Stub the peer's behavior to reflect the desired test conditions.
        when(mockQuorumPeer.shouldUsePortUnification()).thenReturn(false);
        when(mockQuorumPeer.getQuorumListenOnAllIPs()).thenReturn(false);
        when(mockQuorumPeer.getQuorumAddress()).thenReturn(peerConfig.getClientPortAddress());

        Leader leaderRole = null;
        ServerSocket leaderListeningSocket = null;

        try {
            // When: The Leader role is instantiated.
            leaderRole = new Leader(mockQuorumPeer, mockLeaderServer);

            // Then: The internal server socket should be a standard plaintext socket.
            leaderListeningSocket = getLeaderListeningSocket(leaderRole);

            assertNotNull("The leader's listening socket should not be null.", leaderListeningSocket);
            assertEquals("Expected a standard ServerSocket, not a subclass.",
                    ServerSocket.class, leaderListeningSocket.getClass());

            // And: The configuration settings for port unification should have been checked.
            verify(mockQuorumPeer, atLeastOnce()).shouldUsePortUnification();
            verify(mockQuorumPeer, atLeastOnce()).getQuorumListenOnAllIPs();

        } finally {
            // Cleanup: Ensure the socket is closed after the test.
            if (leaderListeningSocket != null) {
                leaderListeningSocket.close();
            }
        }
    }

    /**
     * Loads and parses the test configuration file.
     *
     * @return A configured QuorumPeerConfig object.
     */
    private QuorumPeerConfig loadTestConfiguration() throws IOException, QuorumPeerConfig.ConfigException {
        Properties configProperties = new Properties();
        try (InputStream stream = new FileInputStream(TEST_CONFIG_PATH)) {
            configProperties.load(stream);
        }

        QuorumPeerConfig peerConfig = new QuorumPeerConfig();
        peerConfig.parseProperties(configProperties);
        return peerConfig;
    }

    /**
     * Uses reflection to access the private 'ss' (ServerSocket) field within the Leader instance for verification.
     *
     * @param leader The Leader instance to inspect.
     * @return The internal ServerSocket instance.
     */
    private ServerSocket getLeaderListeningSocket(Leader leader) {
        try {
            Field socketField = Leader.class.getDeclaredField("ss");
            socketField.setAccessible(true); // Grant access to the private field.
            return (ServerSocket) socketField.get(leader);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Failed to inspect Leader's internal state via reflection: " + e.getMessage());
            return null; // Should not be reached.
        }
    }
}