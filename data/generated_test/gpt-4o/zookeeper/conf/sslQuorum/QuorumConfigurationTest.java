package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuorumConfigurationTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    // Test to validate the `sslQuorum` configuration.
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test the `sslQuorum` configuration behavior.
    // 4. Validate the configuration after testing.
    public void testSslQuorumConfiguration() throws IOException {
        // Step 1: Load the configuration properties from file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Use QuorumPeerConfig to parse the loaded properties
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (QuorumPeerConfig.ConfigException e) {
            throw new RuntimeException("Failed to parse configuration properties", e);
        }

        // Step 3: Extract the `sslQuorum` property and validate it
        String sslQuorumValue = props.getProperty("sslQuorum");

        if (sslQuorumValue == null || sslQuorumValue.trim().isEmpty()) {
            // If the `sslQuorum` property is not set or empty, `isSslQuorum()` should return false
            assertFalse(config.isSslQuorum());
        } else {
            String sslQuorumStringValue = sslQuorumValue.trim().toLowerCase();

            // Assert that `sslQuorum` contains a valid boolean value
            assertTrue(sslQuorumStringValue.equals("true") || sslQuorumStringValue.equals("false"));

            // Validate the behavior of `isSslQuorum()` based on the property value
            if (sslQuorumStringValue.equals("true")) {
                assertTrue(config.isSslQuorum());
            } else {
                assertFalse(config.isSslQuorum());
            }
        }
    }
}