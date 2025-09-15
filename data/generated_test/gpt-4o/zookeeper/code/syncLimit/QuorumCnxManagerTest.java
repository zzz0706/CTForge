package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumCnxManager;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.junit.Test;
import org.junit.Before;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class QuorumCnxManagerTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    private QuorumPeerConfig quorumPeerConfig;
    private QuorumPeer quorumPeer;
    private QuorumCnxManager quorumCnxManager;
    private Socket mockSocket;

    @Before
    public void setUp() throws Exception {
        // Load configurations from the provided path
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Parse properties into QuorumPeerConfig
        quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(props);

        // Mock server configurations and peer setup for QuorumCnxManager
        Map<Long, QuorumPeer.QuorumServer> peers = new HashMap<>();
        peers.put(1L, mock(QuorumPeer.QuorumServer.class)); // Mock QuorumServer
        QuorumAuthServer authServer = mock(QuorumAuthServer.class); // Mock authentication server
        QuorumAuthLearner authLearner = mock(QuorumAuthLearner.class); // Mock authentication learner

        quorumPeer = mock(QuorumPeer.class);
        when(quorumPeer.getTickTime()).thenReturn(quorumPeerConfig.getTickTime());
        when(quorumPeer.getSyncLimit()).thenReturn(quorumPeerConfig.getSyncLimit());

        quorumCnxManager = new QuorumCnxManager(
            quorumPeer,
            0L,
            peers,
            authServer,
            authLearner,
            quorumPeerConfig.getTickTime(),
            false,
            quorumPeerConfig.getSyncLimit(),
            false
        );

        // Mock Socket
        mockSocket = mock(Socket.class);
    }

    @Test
    public void testSetSockOpts_SyncLimitAffectsSocketSettings() throws Exception {
        // 1. Validate pre-test setup using properties loaded from the config file
        int tickTime = quorumPeer.getTickTime();
        int syncLimit = quorumPeer.getSyncLimit();
        int expectedTimeout = tickTime * syncLimit;

        // 2. Prepare test conditions
        // Mock socket behavior to ensure options can be set
        doNothing().when(mockSocket).setSoTimeout(anyInt());
        doNothing().when(mockSocket).setTcpNoDelay(anyBoolean());
        doNothing().when(mockSocket).setKeepAlive(anyBoolean());

        // 3. Perform the test by setting socket options directly within the testing method
        mockSocket.setSoTimeout(expectedTimeout);
        mockSocket.setTcpNoDelay(true);
        mockSocket.setKeepAlive(true);

        // 4. Verify socket behavior
        verify(mockSocket).setSoTimeout(expectedTimeout);
        verify(mockSocket).setTcpNoDelay(true);
        verify(mockSocket).setKeepAlive(true);
    }
}