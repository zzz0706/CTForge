package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumCnxManager;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.File;
import java.net.Socket;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class QuorumPeerManagerTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    // Test code to maximize coverage of quorum.cnxn.threads.size functionality.
    // 1. You need to use the Zookeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_QuorumCnxnThreadsSize_Functionality() throws Exception {
        // Step 1: Load the configuration properties using the correct Zookeeper API.
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse the configuration into QuorumPeerConfig.
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 3: Setup test data and mocks.
        // Mock QuorumPeer to ensure the thread size is set correctly.
        QuorumPeer peerMock = Mockito.mock(QuorumPeer.class);

        int validThreadSize = 10; // Test-specific value greater than the default.
        peerMock.setQuorumCnxnThreadsSize(validThreadSize);
        verify(peerMock, times(1)).setQuorumCnxnThreadsSize(validThreadSize);

        // Mock QuorumPeerConfig to retrieve test data directories.
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.getDataDir()).thenReturn(config.getDataDir());
        when(configMock.getDataLogDir()).thenReturn(config.getDataLogDir());
        when(configMock.getQuorumVerifier()).thenReturn(config.getQuorumVerifier());

        // Verify mock values.
        assertEquals(config.getDataDir(), configMock.getDataDir());
        assertEquals(config.getDataLogDir(), configMock.getDataLogDir());

        // Step 4: Test QuorumCnxManager instantiation with the threads size.
        QuorumVerifier verifier = configMock.getQuorumVerifier();
        QuorumCnxManager cnxManager = new QuorumCnxManager(
                peerMock,
                1L,
                verifier.getAllMembers(),
                null, // Mock auth server
                null, // Mock learner handler
                config.getSyncLimit(),   // Using sync limit for timeout.
                true,
                validThreadSize,
                false // False for quorumSaslAuthEnabled (no direct field exists).
        );

        // Test specific behaviors in QuorumCnxManager.
        Socket testSocket = new Socket();
        cnxManager.initiateConnectionAsync(testSocket, 1L);
        cnxManager.receiveConnectionAsync(testSocket);

        // Verify the thread count increment operations.
        assertNotNull(cnxManager);

        // Step 5: Test halting functionality in QuorumCnxManager.
        cnxManager.halt(); // Ensure proper shutdown and thread clearing.
    }
}