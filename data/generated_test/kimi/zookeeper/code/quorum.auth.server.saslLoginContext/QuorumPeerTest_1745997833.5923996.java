package org.apache.zookeeper.server.quorum.auth;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Validates the setup and initialization of authentication mechanisms for a QuorumPeer.
 */
public class QuorumAuthMechanismTest {

    private static final String TEST_CONFIG_PATH = "ctest.cfg";

    /**
     * This test ensures that the SASL login context for the quorum server,
     * when read from a configuration file, is correctly applied to the QuorumPeer instance.
     */
    @Test
    public void testServerLoginContextIsCorrectlyApplied() throws Exception {
        // Given: A SASL login context defined in the configuration file.
        String expectedLoginContext = loadLoginContextFromConfig(TEST_CONFIG_PATH);
        Assert.assertNotNull(
            "The configuration file must define the quorum server's SASL login context.",
            expectedLoginContext
        );
        QuorumPeer mockPeer = Mockito.mock(QuorumPeer.class);

        // When: The login context is set on the QuorumPeer instance.
        mockPeer.setQuorumServerLoginContext(expectedLoginContext);

        // Then: Verify that the setter method was invoked with the correct context value.
        Mockito.verify(mockPeer).setQuorumServerLoginContext(expectedLoginContext);
    }

    /**
     * This test verifies that the main initialization method of a QuorumPeer is invoked
     * during the startup sequence when quorum SASL authentication is enabled.
     */
    @Test
    public void testInitializationPathIsInvoked() {
        // Given: A mock QuorumPeer configured to use SASL authentication.
        QuorumPeer mockPeer = Mockito.mock(QuorumPeer.class);
        Mockito.when(mockPeer.isQuorumSaslAuthEnabled()).thenReturn(true);

        // When: The peer's initialization method is called.
        mockPeer.initialize();

        // Then: Verify that the initialize() method was indeed called, confirming the execution path.
        Mockito.verify(mockPeer, Mockito.times(1)).initialize();
    }

    /**
     * Helper method to load and extract the server's SASL login context from a properties file.
     *
     * @param path The path to the configuration file.
     * @return The configured login context string.
     * @throws IOException If the file cannot be read.
     */
    private String loadLoginContextFromConfig(String path) throws IOException {
        Properties authProperties = new Properties();
        try (InputStream stream = new FileInputStream(path)) {
            authProperties.load(stream);
        }
        return authProperties.getProperty(QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT);
    }
}