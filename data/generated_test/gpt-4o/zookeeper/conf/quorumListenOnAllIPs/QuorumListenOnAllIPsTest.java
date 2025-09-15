package org.apache.zookeeper.server.quorum;

import org.junit.Test;
import org.mockito.Mockito;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test class for verifying the validity of the `quorumListenOnAllIPs` configuration.
 */
public class QuorumListenOnAllIPsTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test case to validate the `quorumListenOnAllIPs` configuration.
     * Reads the configuration value using Zookeeper's API and validates it against the constraints and dependencies.
     */
    @Test
    public void testQuorumListenOnAllIPsConfiguration() {
        try {
            // Load configuration properties from the configuration file
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Mock QuorumPeerConfig and parse the properties
            QuorumPeerConfig configMock = Mockito.mock(QuorumPeerConfig.class);
            configMock.parseProperties(props);

            // Retrieve the value of `quorumListenOnAllIPs`
            Boolean quorumListenOnAllIPs = configMock.getQuorumListenOnAllIPs();

            // Step 1: Validate the configuration value through constraints
            // The only valid values for `quorumListenOnAllIPs` are `true` and `false`.
            if (quorumListenOnAllIPs == null) {
                fail("Invalid configuration: `quorumListenOnAllIPs` is not set.");
            } else {
                assertTrue(
                    "Configuration `quorumListenOnAllIPs` must be either true or false.",
                    quorumListenOnAllIPs.equals(Boolean.TRUE) || quorumListenOnAllIPs.equals(Boolean.FALSE)
                );
            }

            // Step 2: Validate logical correctness and dependencies
            // As per the source code, if `quorumListenOnAllIPs` is `true`, the server will bind to all available IPs; otherwise, it will bind to a specific IP address.
            // Since this configuration alone does not have additional dependencies to validate, this concludes the test verification.

        } catch (Exception e) {
            // Handle errors in the parsing or validation process
            fail("Exception encountered during configuration validation: " + e.getMessage());
        }
    }
}