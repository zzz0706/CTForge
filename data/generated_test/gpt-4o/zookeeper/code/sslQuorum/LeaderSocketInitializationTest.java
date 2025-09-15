package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

public class LeaderSocketInitializationTest {

    @Test
    public void testLeaderSocketInitialization_withSSLQuorumDisabled() {
        try {
            // 1. Load configuration values using the Zookeeper API (mocked for testing purposes).
            QuorumPeer quorumPeerMock = mock(QuorumPeer.class);
            
            // Mock the `isSslQuorum` method to simulate sslQuorum=false.
            when(quorumPeerMock.isSslQuorum()).thenReturn(false);
            when(quorumPeerMock.shouldUsePortUnification()).thenReturn(false);
            when(quorumPeerMock.getQuorumListenOnAllIPs()).thenReturn(false);
            when(quorumPeerMock.getQuorumAddress()).thenReturn(null); // Mock configuration dependencies.

            // 2. Prepare test conditions.
            LeaderZooKeeperServer leaderZooKeeperServerMock = mock(LeaderZooKeeperServer.class);

            // 3. Test code.
            Leader leaderInstance = new Leader(quorumPeerMock, leaderZooKeeperServerMock);

            // Retrieve the socket directly via the Leader instance's field using reflection (validating encapsulated behavior).
            // Note: In real scenarios, ensure visibility or access methods for private fields to perform assertions safely.
            java.lang.reflect.Field socketField = Leader.class.getDeclaredField("ss");
            socketField.setAccessible(true);
            Object socketInstance = socketField.get(leaderInstance);

            // Assert that the socket used is an instance of the regular ServerSocket.
            assertTrue("Expected ServerSocket instance when SSL Quorum is disabled.",
                    socketInstance instanceof java.net.ServerSocket);

        } catch (IOException | NoSuchFieldException | IllegalAccessException e) {
            fail("Unexpected exception during test execution: " + e.getMessage());
        }
    }
}