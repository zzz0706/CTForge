package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class QuorumAuthConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to validate the quorum.auth.enableSasl configuration value
     */
    @Test
    public void testQuorumAuthEnableSaslConfig() throws Exception {
        // Step 1: Load the configuration file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse the configuration using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 3: Extract the quorum.auth.enableSasl configuration value
        String quorumAuthEnableSasl = props.getProperty("quorum.auth.enableSasl");

        // Step 4: Validate the configuration value
        // Since the source code indicates that this is a boolean configuration, validate it's either "true" or "false".
        assertTrue(
            "quorum.auth.enableSasl value is invalid. Expected 'true' or 'false'.",
            "true".equalsIgnoreCase(quorumAuthEnableSasl) || "false".equalsIgnoreCase(quorumAuthEnableSasl)
        );
    }
}