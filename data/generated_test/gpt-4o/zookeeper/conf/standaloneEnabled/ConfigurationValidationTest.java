package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

public class ConfigurationValidationTest {
    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to ensure that the `standaloneEnabled` configuration is valid and adheres to constraints and dependencies.
     */
    @Test
    public void testStandaloneEnabledConfig() {
        Properties props = new Properties();

        // Step 1: Load configuration from file.
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        } catch (IOException e) {
            fail("Failed to load configuration file: " + e.getMessage());
        }

        // Step 2: Retrieve the `standaloneEnabled` configuration value.
        String standaloneEnabledValue = props.getProperty("standaloneEnabled");

        // Step 3: Validate the configuration.
        try {
            // Case 1: `standaloneEnabled` is null or empty (use default value, assumed as "true").
            if (standaloneEnabledValue == null || standaloneEnabledValue.isEmpty()) {
                standaloneEnabledValue = "true"; // Default fallback for backward compatibility.
            }

            // Case 2: Verify that the value is either "true" or "false" (boolean constraints).
            if (!standaloneEnabledValue.equals("true") && !standaloneEnabledValue.equals("false")) {
                fail("Invalid value for `standaloneEnabled`. Expected 'true' or 'false', but got: " + standaloneEnabledValue);
            }

            // Case 3: Validate dependencies
            if (standaloneEnabledValue.equals("false")) {
                // When `standaloneEnabled` is set to false, ensure quorum-related configurations exist.
                String serverId = props.getProperty("serverId"); // Example expected dependency.
                String quorumPeers = props.getProperty("server"); // Example quorum configuration.

                if (serverId == null || serverId.isEmpty()) {
                    fail("Dependency error: `serverId` must be configured when `standaloneEnabled` is false.");
                }

                if (quorumPeers == null || quorumPeers.isEmpty()) {
                    fail("Dependency error: One or more `server` entries must be configured when `standaloneEnabled` is false.");
                }
            }

        } catch (Exception e) {
            fail("Exception during validation: " + e.getMessage());
        }

        // Step 4: Test successful load and valid property usage.
        assertTrue("Validation of `standaloneEnabled` succeeded.", true);
    }
}