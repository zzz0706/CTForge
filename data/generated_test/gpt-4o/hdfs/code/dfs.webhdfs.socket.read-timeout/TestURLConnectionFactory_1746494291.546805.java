package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestURLConnectionFactory {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_newOAuth2URLConnectionFactory_with_missing_ssl_config() {
        // Prepare the test conditions.
        // Create a Configuration object without SSL-related configurations.
        Configuration conf = new Configuration();
        conf.unset("dfs.webhdfs.oauth.enabled"); // Explicitly unset OAuth2-related configuration.

        try {
            // Test code.
            // Call the newOAuth2URLConnectionFactory method with connectTimeout=5000 and readTimeout=5000.
            URLConnectionFactory.newOAuth2URLConnectionFactory(5000, 5000, conf);

            // If no exception is thrown, the test should fail because SSL config is missing.
            fail("Expected IOException was not thrown when missing SSL configuration.");
        } catch (IOException e) {
            // Capture any thrown exception and verify that it contains the expected error message.
            assertTrue("Exception does not contain expected message about missing SSL configuration.",
                    e.getMessage().contains("Unable to load OAuth2 connection factory."));
        }

        // Code after testing (e.g., clean up or finalize resources, if necessary).
        // No cleanup required here as the test uses disposable resources only.
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_initialize_with_oauth_enabled_configuration() throws IOException {
        // Prepare the test conditions.
        // Create a Configuration object with OAuth2 enabled and valid timeouts.
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.webhdfs.oauth.enabled", true); // Enable OAuth2.
        conf.setInt("dfs.webhdfs.socket.connect-timeout", 5000); // Set connect timeout.
        conf.setInt("dfs.webhdfs.socket.read-timeout", 5000); // Set read timeout.

        // Create a WebHdfsFileSystem instance and a mock URI needed for initialization.
        URI mockURI = URI.create("webhdfs://localhost:50070");
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();

        // Test code.
        webHdfsFileSystem.initialize(mockURI, conf);

        // Code after testing (e.g., validating and resource cleanup).
        // Assert that the WebHdfsFileSystem is properly initialized, verifying the OAuth2 connection factory.
        assertTrue("Connection factory should be OAuth2 based when OAuth2 is enabled in configuration.",
                webHdfsFileSystem.getConf().get("dfs.webhdfs.oauth.enabled").equals("true"));
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_initialize_without_oauth_enabled_configuration() throws IOException {
        // Prepare the test conditions.
        // Create a Configuration object with OAuth2 explicitly disabled and valid timeouts.
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.webhdfs.oauth.enabled", false); // Disable OAuth2.
        conf.setInt("dfs.webhdfs.socket.connect-timeout", 5000); // Set connect timeout.
        conf.setInt("dfs.webhdfs.socket.read-timeout", 5000); // Set read timeout.

        // Create a WebHdfsFileSystem instance and a mock URI needed for initialization.
        URI mockURI = URI.create("webhdfs://localhost:50070");
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();

        // Test code.
        webHdfsFileSystem.initialize(mockURI, conf);

        // Code after testing (e.g., validating and resource cleanup).
        // Assert that the WebHdfsFileSystem is initialized without OAuth2 connection factory.
        assertTrue("Connection factory should not be OAuth2 based when OAuth2 is disabled in configuration.",
                webHdfsFileSystem.getConf().get("dfs.webhdfs.oauth.enabled").equals("false"));
    }
}