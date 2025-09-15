package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class QuorumAuthConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to validate the `quorum.auth.serverRequireSasl` configuration.
     */
    @Test
    public void testQuorumAuthServerRequireSaslConfig() throws Exception {
        // Step 1: Load the configuration
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse the configuration using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 3: Retrieve the value of the `quorum.auth.serverRequireSasl` property
        String quorumAuthServerRequireSasl = props.getProperty(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);

        // Step 4: Validate the configuration value:
        // - The value must be either "true" or "false" (valid boolean values)
        // - Any other value or absence of the configuration is considered invalid

        if (quorumAuthServerRequireSasl != null) {
            quorumAuthServerRequireSasl = quorumAuthServerRequireSasl.trim().toLowerCase();
            assertTrue(
                    "The configuration 'quorum.auth.serverRequireSasl' must be 'true' or 'false'.",
                    "true".equals(quorumAuthServerRequireSasl) || "false".equals(quorumAuthServerRequireSasl)
            );
        } else {
            // Fail the test if the configuration is missing
            throw new IllegalArgumentException("The configuration 'quorum.auth.serverRequireSasl' is missing.");
        }

        // Log a message for successful validation
        System.out.println("Configuration 'quorum.auth.serverRequireSasl' is valid with value: " + quorumAuthServerRequireSasl);
    }
}