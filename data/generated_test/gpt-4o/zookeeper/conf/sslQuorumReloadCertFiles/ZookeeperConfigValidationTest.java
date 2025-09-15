package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ZookeeperConfigValidationTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to validate the configuration value for `sslQuorumReloadCertFiles`
     * and its dependencies and constraints.
     */
    @Test
    public void testSslQuorumReloadCertFilesConfiguration() throws Exception {
        // Step 1: Load properties from the configuration file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse the properties into a QuorumPeerConfig object
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 3: Retrieve the sslQuorumReloadCertFiles configuration
        String sslQuorumReloadCertFilesValue = props.getProperty("sslQuorumReloadCertFiles");

        // Step 4: Validate the configuration value based on constraints and dependencies
        // Constraint: sslQuorumReloadCertFiles is a boolean value, so it should be either "true" or "false".
        Assert.assertTrue(
            "Invalid value for sslQuorumReloadCertFiles. Expected true/false but received: "
                + sslQuorumReloadCertFilesValue,
            sslQuorumReloadCertFilesValue.equalsIgnoreCase("true")
                || sslQuorumReloadCertFilesValue.equalsIgnoreCase("false")
        );

        // Step 5: Validate the functionality propagations and dependencies if necessary
        if (sslQuorumReloadCertFilesValue.equalsIgnoreCase("true")) {
            // Additionally, validate related configurations like sslKeystoreLocation and sslTruststoreLocation
            String sslKeystoreLocation = props.getProperty("sslKeystoreLocation");
            String sslTruststoreLocation = props.getProperty("sslTruststoreLocation");

            Assert.assertNotNull(
                "sslKeystoreLocation cannot be null when sslQuorumReloadCertFiles is true.",
                sslKeystoreLocation
            );
            Assert.assertNotNull(
                "sslTruststoreLocation cannot be null when sslQuorumReloadCertFiles is true.",
                sslTruststoreLocation
            );

            // Validate whether the file paths are valid
            Assert.assertTrue(
                "Invalid sslKeystoreLocation path: " + sslKeystoreLocation,
                isValidPath(sslKeystoreLocation)
            );
            Assert.assertTrue(
                "Invalid sslTruststoreLocation path: " + sslTruststoreLocation,
                isValidPath(sslTruststoreLocation)
            );
        }
    }

    /**
     * Utility method to validate whether a given file path is valid.
     *
     * @param path The file path to validate.
     * @return true if the path is valid, false otherwise.
     */
    private boolean isValidPath(String path) {
        // Check if the path points to an existing file or directory (basic validation)
        return path != null && new java.io.File(path).exists();
    }
}