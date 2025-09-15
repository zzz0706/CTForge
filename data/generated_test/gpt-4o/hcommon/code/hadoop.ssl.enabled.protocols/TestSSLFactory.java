package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import javax.net.ssl.SSLEngine;

import static org.junit.Assert.assertArrayEquals;

public class TestSSLFactory {
    
    @Test
    public void test_SSLFactory_initialization_with_valid_mode_and_configuration() throws Exception {
        // Fetch configuration values using API
        Configuration conf = new Configuration();
        String[] configuredProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

        // Prepare input conditions for unit testing
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // Initialize the SSLFactory
        sslFactory.init();

        // Verify that the fetched protocols were correctly propagated
        SSLEngine sslEngine = sslFactory.createSSLEngine();
        String[] enabledProtocols = sslEngine.getEnabledProtocols();

        // Test assertion to ensure correctness
        assertArrayEquals("The enabled protocols should match the configured values.", configuredProtocols, enabledProtocols);
    }
}