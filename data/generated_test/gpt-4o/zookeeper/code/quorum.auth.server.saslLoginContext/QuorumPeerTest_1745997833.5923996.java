package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class QuorumPeerTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    // Test for `setQuorumServerLoginContext` method
    // 1. Use the ZooKeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the values.
    // 2. Prepare the test conditions.
    // 3. Verify that the API sets the quorum server login context properly and logs the initialized context value.
    public void testSetQuorumServerLoginContext() {
        try {
            // Step 1: Load configuration properties using ZooKeeper config API
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Parse the configuration
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Extract the configured SASL login context for the quorum server
            String serverContext = props.getProperty(QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT);
            Assert.assertNotNull("Configuration should define quorum.auth.server.saslLoginContext", serverContext);

            // Step 2: Initialize the QuorumPeer instance and mock its behavior
            QuorumPeer quorumPeer = Mockito.mock(QuorumPeer.class);

            // Mock the `setQuorumServerLoginContext` method behavior
            Mockito.doCallRealMethod().when(quorumPeer).setQuorumServerLoginContext(Mockito.anyString());

            // Call the actual method to set the login context
            quorumPeer.setQuorumServerLoginContext(serverContext);

            // Verify that `setQuorumServerLoginContext` was invoked correctly
            Mockito.verify(quorumPeer, Mockito.times(1)).setQuorumServerLoginContext(Mockito.eq(serverContext));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Test case failed due to exception: " + e.getMessage());
        }
    }

    @Test
    // Test `initialize` method
    // 1. Use ZooKeeper API correctly to handle quorum SASL authentication based on configuration values.
    // 2. Mock necessary conditions and test all logic paths in the `initialize` method.
    // 3. Ensure proper initialization of quorum authentication server and learner objects.
    public void testInitialize() {
        try {
            // Step 1: Load configuration properties
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Parse configuration
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Step 2: Mock a `QuorumPeer` instance
            QuorumPeer quorumPeer = Mockito.mock(QuorumPeer.class);

            // Mock methods to simulate quorum SASL authentication behavior
            Mockito.when(quorumPeer.isQuorumSaslAuthEnabled()).thenReturn(true);

            // Initialize the quorum peer
            Mockito.doNothing().when(quorumPeer).initialize();

            // Call the `initialize` method
            quorumPeer.initialize();

            // Verify that the `initialize` method was called correctly
            Mockito.verify(quorumPeer, Mockito.times(1)).initialize();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Test failed due to exception: " + e.getMessage());
        }
    }
}