package org.apache.zookeeper.server.quorum;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Validates the lifecycle and startup behavior of the Quorum Connection Manager (QuorumCnxManager).
 */
public class ConnectionManagerLifecycleTest {

    /**
     * This test verifies that during startup, the QuorumCnxManager correctly checks
     * the port unification setting when it is disabled. The manager's main loop
     * is run in a separate thread to allow for verification from the main test thread.
     */
    @Test
    public void testManagerStartupWithPortUnificationDisabled() throws InterruptedException {
        // Given: A set of mock objects and parameters to configure the connection manager.
        long localPeerId = 1L;
        QuorumPeer mockPeer = mock(QuorumPeer.class);
        QuorumAuthServer mockAuthServer = mock(QuorumAuthServer.class);
        QuorumAuthLearner mockAuthLearner = mock(QuorumAuthLearner.class);

        // Define the cluster topology with one other peer.
        Map<Long, QuorumPeer.QuorumServer> viewOfClusterMembers = Collections.singletonMap(
            2L, new QuorumPeer.QuorumServer(2L, new InetSocketAddress("localhost", 2182))
        );

        // Stub the behavior of the mock peer to disable port unification.
        when(mockPeer.shouldUsePortUnification()).thenReturn(false);
        when(mockPeer.getElectionAddress()).thenReturn(new InetSocketAddress("localhost", 2181));
        when(mockPeer.getView()).thenReturn(viewOfClusterMembers);

        // Instantiate the connection manager with the prepared test configuration.
        QuorumCnxManager connectionManager = new QuorumCnxManager(
            mockPeer,
            localPeerId,
            viewOfClusterMembers,
            mockAuthServer,
            mockAuthLearner,
            1000, // socketTimeout
            false, // listener threads daemon
            1000, // connectTimeout
            false  // sync limit check
        );

        // When: The connection manager's main execution loop is started in a background thread.
        Thread managerThread = new Thread(connectionManager::run, "ConnectionManagerThread");
        
        try {
            managerThread.start();
            // Allow a brief moment for the thread to initialize its socket and check the config.
            Thread.sleep(500);

            // Then: Verify from the main thread that the startup logic correctly queried the port unification setting.
            verify(mockPeer, atLeastOnce()).shouldUsePortUnification();

        } finally {
            // Cleanup: Ensure the background thread is stopped and cleaned up, even if assertions fail.
            managerThread.interrupt();
            managerThread.join(1000); // Wait for the thread to terminate.
        }
    }
}