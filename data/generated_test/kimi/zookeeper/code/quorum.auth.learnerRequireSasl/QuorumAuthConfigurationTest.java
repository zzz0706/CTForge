package org.apache.zookeeper.server.quorum.auth;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Validates the parsing of quorum authentication settings from a configuration file.
 */
public class QuorumAuthConfigurationTest {

    private static final String TEST_CONFIG_PATH = "ctest.cfg";

    /**
     * This test ensures that when the configuration enables quorum SASL authentication
     * but does not require it for learners, the settings are parsed correctly.
     */
    @Test
    public void testLearnerSaslRequirementConfiguration() throws Exception {
        // Given a configuration file with specific SASL settings.
        // When the configuration is loaded and parsed.
        QuorumPeerConfig authConfig = loadAuthConfig(TEST_CONFIG_PATH);

        // Then the parsed settings should reflect the intended authentication policy.
        boolean isQuorumSaslEnabled = authConfig.isQuorumSaslAuthEnabled();
        boolean isLearnerSaslRequired = authConfig.isQuorumLearnerSaslRequired();

        // Validate the expected outcome for a mixed-mode SASL environment.
        assertTrue(
            "Quorum SASL authentication should be globally enabled in this scenario.",
            isQuorumSaslEnabled
        );
        assertFalse(
            "SASL authentication should not be strictly required for learners in this scenario.",
            isLearnerSaslRequired
        );
    }

    /**
     * Helper method to load and parse a ZooKeeper configuration file.
     *
     * @param configPath The path to the configuration file.
     * @return A parsed QuorumPeerConfig object.
     * @throws IOException If there is an error reading the file.
     * @throws QuorumPeerConfig.ConfigException If the configuration is invalid.
     */
    private QuorumPeerConfig loadAuthConfig(String configPath) throws IOException, QuorumPeerConfig.ConfigException {
        Properties authProperties = new Properties();
        try (InputStream stream = new FileInputStream(configPath)) {
            authProperties.load(stream);
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(authProperties);
        return config;
    }
}