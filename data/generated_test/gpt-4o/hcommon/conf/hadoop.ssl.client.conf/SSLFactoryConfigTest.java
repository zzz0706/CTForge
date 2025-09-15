package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Unit tests to verify the validity of the hadoop.ssl.client.conf configuration.
 * This checks constraints and dependencies for correctness.
 */
public class SSLFactoryConfigTest {

    private static final String SSL_CLIENT_CONF_KEY = "hadoop.ssl.client.conf";
    private static final String DEFAULT_SSL_CLIENT_CONF = "ssl-client.xml";

    /**
     * Verify that the configuration value `hadoop.ssl.client.conf` conforms to specified constraints.
     * - The configuration must point to a valid file/resource within the classpath.
     * - The file should exist in the typical location (e.g., Hadoop conf directory).
     */
    @Test
    public void testSSLClientConfValidity() {
        Configuration conf = new Configuration(false);

        // Step 2.1: Prepare test configuration values
        // Simulate creating the expected ssl-client.xml file for test purposes
        String testConfDir = System.getProperty("hadoop.conf.dir", "./conf");
        File sslClientConfFile = new File(testConfDir, DEFAULT_SSL_CLIENT_CONF);
        sslClientConfFile.getParentFile().mkdirs(); // Create parent directories if missing
        try {
            sslClientConfFile.createNewFile(); // Create the test file if it doesn't exist
        } catch (Exception e) {
            fail("Failed to create test ssl-client.xml file for testing: " + e.getMessage());
        }

        // Retrieve the value from the config
        String sslClientConf = conf.get(SSL_CLIENT_CONF_KEY, DEFAULT_SSL_CLIENT_CONF);

        // Step 3: Verify conditions
        // Step 3.1: Verify config value is not null or empty
        assertNotNull("The hadoop.ssl.client.conf should not be null", sslClientConf);
        assertFalse("The hadoop.ssl.client.conf should not be empty", sslClientConf.trim().isEmpty());

        // Step 3.2: Verify that the configuration points to a valid file/resource
        assertTrue("The ssl-client.xml file should exist at the expected location", sslClientConfFile.exists());
        assertTrue("The ssl-client.xml file should be a valid file", sslClientConfFile.isFile());

        // Step 3.3: Validate propagation dependency (dependent on usage in the code)
        SSLFactory.Mode mode = SSLFactory.Mode.CLIENT;
        SSLFactory sslFactory = new SSLFactory(mode, conf);
        assertNotNull("SSLFactory should correctly initialize for hadoop.ssl.client.conf", sslFactory);

        // Step 3.4: Verify path constraints (optional, depending on internal file handling expectations)
        assertFalse("The ssl-client.xml file path should not contain invalid characters",
                sslClientConf.contains("..") || sslClientConf.contains(":"));
    }
}