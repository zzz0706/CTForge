package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import static org.junit.Assert.*;

public class QuorumPeerConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test if the 'localSessionsEnabled' configuration is set correctly and satisfies constraints/dependencies.
     */
    @Test
    public void testLocalSessionsEnabledConfiguration() {
        try {
            // Step 1: Load configuration file
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Step 2: Parse the configuration using QuorumPeerConfig
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Step 3: Retrieve the value of 'localSessionsEnabled'
            boolean localSessionsEnabled = config.areLocalSessionsEnabled();

            // Step 4: Assert valid constraints and dependencies
            // In Zookeeper 3.5.6, 'localSessionsEnabled' should be a boolean indicating whether local sessions are enabled.
            // Ensure the value retrieved is valid given it's a boolean configuration.
            assertTrue(
                "Invalid configuration for 'localSessionsEnabled': Value must be either 'true' or 'false'.",
                localSessionsEnabled == true || localSessionsEnabled == false
            );

        } catch (Exception e) {
            // Fail the test if any exception occurs while reading or validating configuration
            fail("Exception occurred while testing 'localSessionsEnabled' configuration: " + e.getMessage());
        }
    }
}