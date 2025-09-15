package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Test class for validating the configuration quorum.auth.learner.saslLoginContext
 * based on constraints and dependencies.
 */
public class QuorumAuthLearnerSaslLoginContextTest {

    private static final String CONFIG_PATH = "ctest.cfg";


    @Test
    public void testQuorumAuthLearnerSaslLoginContextConfiguration() throws Exception {
        // Step 1: Load the configuration file
        Properties props = new Properties();
        try (InputStream inputStream = new FileInputStream(CONFIG_PATH)) {
            props.load(inputStream);
        }

        // Step 2: Retrieve the configuration value
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);
        String saslLoginContext = props.getProperty("quorum.auth.learner.saslLoginContext");
        // Step 3: Validate the configuration value based on constraints and dependencies
        assertTrue("Configuration quorum.auth.learner.saslLoginContext should not be null or empty.",
                saslLoginContext != null && !saslLoginContext.isEmpty());

    }

}