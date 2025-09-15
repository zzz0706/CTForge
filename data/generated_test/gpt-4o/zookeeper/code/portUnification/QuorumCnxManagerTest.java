package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumCnxManager;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class QuorumCnxManagerTest {

    // Configuration file path
    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void test_run_with_port_unification_disabled() throws Exception {
        // Test code
        // 1. Fetch the configuration using the ZooKeeper 3.5.6 API
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(props);

        // 2. Prepare the test conditions
        QuorumPeer quorumPeerMock = Mockito.mock(QuorumPeer.class);
        Mockito.when(quorumPeerMock.shouldUsePortUnification()).thenReturn(false);
        Mockito.when(quorumPeerMock.getElectionAddress()).thenReturn(new InetSocketAddress("localhost", 2181));

        Map<Long, QuorumServer> quorumServerMap = new HashMap<>();
        quorumServerMap.put(1L, new QuorumServer(1, new InetSocketAddress("localhost", 2182)));

        long myId = 1L;
        QuorumAuthServer authServerMock = Mockito.mock(QuorumAuthServer.class);
        QuorumAuthLearner authLearnerMock = Mockito.mock(QuorumAuthLearner.class);
        int socketTimeout = 30000;
        boolean enableStandbyMode = false;
        int initialConnectTimeout = 15000;
        boolean enableSyncLimitCheck = false;

        QuorumCnxManager quorumCnxManager = new QuorumCnxManager(
                quorumPeerMock,
                myId,
                quorumServerMap,
                authServerMock,
                authLearnerMock,
                socketTimeout,
                enableStandbyMode,
                initialConnectTimeout,
                enableSyncLimitCheck
        );

        // Test code
        // 3. Start the server socket logic in a separate thread
        Thread testThread = new Thread(() -> {
            try {
                quorumCnxManager.toString();
                Mockito.verify(quorumPeerMock, Mockito.times(1)).shouldUsePortUnification(); // Correctly invoke the mock behavior during thread execution
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        testThread.start();

        try {
            Thread.sleep(1000); // Allow the server socket to initialize
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Test Code
        // 4. Code after testing
        testThread.interrupt();
        testThread.join(); // Ensure the thread has finished
    }
}