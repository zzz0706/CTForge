package org.apache.zookeeper.server.quorum.auth;

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
     * Test to validate the "quorum.auth.learnerRequireSasl" configuration.
     * The test ensures that the value of the configuration satisfies its constraints and dependencies.
     */
    @Test
    public void testQuorumAuthLearnerRequireSaslConfiguration() throws Exception {
        // Step 1: Load the properties file and parse the configuration.
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 2: Fetch the value of the "quorum.auth.learnerRequireSasl" configuration.
        String quorumLearnerRequireSaslValue = props.getProperty(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED);

        // Step 3: Validate the configuration value.
        // Since "quorum.auth.learnerRequireSasl" is understood to be a boolean value from the source code,
        // we validate that the value is either "true" or "false".
        assertTrue(
            String.format("Invalid value for configuration '%s'. Expected 'true' or 'false', but found: '%s'",
                QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED, quorumLearnerRequireSaslValue),
            quorumLearnerRequireSaslValue != null &&
            (quorumLearnerRequireSaslValue.equalsIgnoreCase("true") || quorumLearnerRequireSaslValue.equalsIgnoreCase("false"))
        );
    }
}