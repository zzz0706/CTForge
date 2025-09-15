package org.apache.hadoop.crypto.random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.random.OsSecureRandom;
import org.junit.Test;

import java.io.File;

public class TestOsSecureRandom {
    /**
     * Test case: Ensure the nextBytes method throws an exception when the specified random device file path is inaccessible.
     * Objective: Validate that the configuration value for `hadoop.security.random.device.file.path` is handled correctly
     *            and an exception is thrown when the file is inaccessible during the nextBytes operation.
     * Prerequisites: The configuration key 'hadoop.security.random.device.file.path' must point to a nonexistent or inaccessible file.
     */
    @Test
    public void test_nextBytes_invalidConfig() {

        Configuration conf = new Configuration();
        String defaultInvalidPath = "/nonexistent/urandom";
        conf.set("hadoop.security.random.device.file.path", defaultInvalidPath);
        String invalidFilePath = conf.get("hadoop.security.random.device.file.path");

        File file = new File(invalidFilePath);
        // Update validation logic to avoid the need for external conditions
        if (file.exists()) {
            throw new AssertionError("The file should not exist for the test to be valid. Unexpected accessible file path: " + invalidFilePath);
        }

        OsSecureRandom osSecureRandom = new OsSecureRandom();
        osSecureRandom.setConf(conf);

        // Prepare a byte array for the nextBytes operation
        byte[] byteArray = new byte[16];

        try {
            // Call the nextBytes method
            osSecureRandom.nextBytes(byteArray);

            // If no exception is thrown, the test fails
            throw new AssertionError("Expected a RuntimeException due to inaccessible random device file.");
        } catch (RuntimeException e) {
            // Verify the exception indicates an issue with file access
            assert e.getMessage() == null || e.getMessage().contains("failed to fill reservoir") 
                : "Unexpected exception message: " + e.getMessage();
        }

    }
}