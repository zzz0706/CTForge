package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class ReconfigEnabledConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to verify the correctness of the `reconfigEnabled` configuration.
     * Steps:
     * 1. Load the configuration from the specified config file.
     * 2. Validate the `reconfigEnabled` value based on the constraints and dependencies specified.
     */
    @Test
    public void testReconfigEnabledConfig() {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(CONFIG_PATH)) {
            // Load configuration properties from file
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error reading configuration file: " + CONFIG_PATH, e);
        }

        // Step 1: Extract the "reconfigEnabled" configuration value from the Properties object
        String reconfigEnabledValue = props.getProperty("reconfigEnabled");

        // Step 2: Validate the extracted configuration value
        validateReconfigEnabled(reconfigEnabledValue);
    }

    /**
     * Validates the `reconfigEnabled` configuration value.
     *
     * @param reconfigEnabledValue the value of the `reconfigEnabled` configuration read from the file.
     */
    private void validateReconfigEnabled(String reconfigEnabledValue) {
        // Configuration constraint:
        // - Acceptable values are: "true", "false" (case-sensitive as per Java convention).
        // - If the value is not explicitly set, it defaults to "false".

        if (reconfigEnabledValue == null || reconfigEnabledValue.isEmpty()) {
            // Assume default value if the configuration is not defined
            reconfigEnabledValue = "false";
        }

        // Assert that the value is valid
        assertTrue(
            "Invalid value for `reconfigEnabled`, expected 'true' or 'false', but got: " + reconfigEnabledValue,
            "true".equals(reconfigEnabledValue) || "false".equals(reconfigEnabledValue)
        );
    }
}