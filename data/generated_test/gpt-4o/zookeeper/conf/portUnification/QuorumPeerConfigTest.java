package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

public class QuorumPeerConfigTest {
    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to validate portUnification configuration.
     * This test reads the configuration file and checks whether the portUnification configuration is valid.
     */
    @Test
    public void testPortUnificationConfiguration() {
        try {
            // Step 1: Load configuration properties from file
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Step 2: Parse properties using QuorumPeerConfig
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Step 3: Assess constraints and dependencies of the portUnification configuration
            // Configuration: portUnification
            String portUnificationConfigValue = props.getProperty("portUnification");

            // Validate if portUnification is set and whether it holds a valid boolean value
            if (portUnificationConfigValue == null || portUnificationConfigValue.isEmpty()) {
                fail("portUnification configuration is missing or empty.");
            } else {
                boolean isValidValue = portUnificationConfigValue.equalsIgnoreCase("true") ||
                                       portUnificationConfigValue.equalsIgnoreCase("false");
                assertTrue("portUnification configuration must be a valid boolean value (true or false).",
                        isValidValue);
            }
        } catch (IOException e) {
            fail("IOException occurred while reading the configuration file: " + e.getMessage());
        } catch (Exception e) {
            fail("Exception occurred while parsing the configuration: " + e.getMessage());
        }
    }
}