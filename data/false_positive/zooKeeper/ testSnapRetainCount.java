package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class  testSnapRetainCount {
    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to verify the validity of the `autopurge.snapRetainCount` configuration.
     * Steps:
     * 1. Read the configuration value from a file.
     * 2. Validate if the configuration value meets the constraints.
     * 3. Assert and handle violations of the constraints.
     */
    @Test
    public void testSnapRetainCountConfiguration() {
        Properties properties = new Properties();
        try (FileInputStream inputStream = new FileInputStream(CONFIG_PATH)) {
            // Load the properties from the configuration file
            properties.load(inputStream);
        } catch (IOException e) {
            fail("Failed to load configuration file: " + e.getMessage());
        }

        // Get the autopurge.snapRetainCount value from the configuration
        String snapRetainCountStr = properties.getProperty("autopurge.snapRetainCount");

        // Validate constraints for `autopurge.snapRetainCount`
        if (snapRetainCountStr != null) {
            try {
                int snapRetainCount = Integer.parseInt(snapRetainCountStr.trim());

                // Validation Step 1: Check if it meets the minimum value constraint
                assertTrue(
                    "The value of 'autopurge.snapRetainCount' must be greater than or equal to 1.",
                    snapRetainCount >= 1
                );
            } catch (NumberFormatException e) {
                fail("'autopurge.snapRetainCount' must be a valid integer. Provided value: " + snapRetainCountStr);
            }
        } else {
            fail("Property 'autopurge.snapRetainCount' is missing in the configuration file.");
        }
    }
}