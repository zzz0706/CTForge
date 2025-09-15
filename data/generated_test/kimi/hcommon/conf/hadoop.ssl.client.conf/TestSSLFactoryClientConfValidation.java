package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class TestSSLFactoryClientConfValidation {

    private Configuration conf;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration(false);
        // Create a temporary directory to act as classpath root
        tempDir = Files.createTempDirectory("test-ssl");
        // Create a dummy ssl-client.xml file in the temp directory
        Path sslClientFile = tempDir.resolve("ssl-client.xml");
        try (OutputStream out = Files.newOutputStream(sslClientFile)) {
            out.write("<configuration/>".getBytes());
        }
        // Add the temp directory to the classpath
        Thread.currentThread().setContextClassLoader(
            new java.net.URLClassLoader(
                new java.net.URL[]{tempDir.toUri().toURL()},
                Thread.currentThread().getContextClassLoader()
            )
        );
    }

    @After
    public void tearDown() throws IOException {
        conf = null;
        // Clean up temp directory
        for (File f : tempDir.toFile().listFiles()) {
            f.delete();
        }
        Files.deleteIfExists(tempDir);
    }

    /**
     * Test that hadoop.ssl.client.conf points to a file that can be found
     * on the classpath.
     */
    @Test
    public void testSSLClientConfResourceExists() {
        String sslClientConf = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY, "ssl-client.xml");
        assertNotNull("hadoop.ssl.client.conf must be defined", sslClientConf);

        // Verify the resource exists on the classpath
        URL resource = Thread.currentThread().getContextClassLoader().getResource(sslClientConf);
        assertNotNull("SSL client configuration file '" + sslClientConf + "' not found on classpath", resource);

        // Ensure the resource is actually a file (not a directory)
        File file = new File(resource.getFile());
        assertTrue("SSL client configuration file '" + sslClientConf + "' is not a regular file", file.isFile());
    }

    /**
     * Test that hadoop.ssl.client.conf is not empty.
     */
    @Test
    public void testSSLClientConfNotEmpty() {
        String sslClientConf = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY, "ssl-client.xml");
        assertFalse("hadoop.ssl.client.conf must not be empty", sslClientConf.trim().isEmpty());
    }

    /**
     * Test that hadoop.ssl.client.conf ends with .xml as expected.
     */
    @Test
    public void testSSLClientConfEndsWithXml() {
        String sslClientConf = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY, "ssl-client.xml");
        assertTrue("hadoop.ssl.client.conf should end with '.xml'", sslClientConf.toLowerCase().endsWith(".xml"));
    }

    /**
     * Test that hadoop.ssl.client.conf does not contain invalid characters.
     */
    @Test
    public void testSSLClientConfValidFilename() {
        String sslClientConf = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY, "ssl-client.xml");
        assertTrue("hadoop.ssl.client.conf contains invalid characters",
                   sslClientConf.matches("[a-zA-Z0-9._-]+"));
    }
}