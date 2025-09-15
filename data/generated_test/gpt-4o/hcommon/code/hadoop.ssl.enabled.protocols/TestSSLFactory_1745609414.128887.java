package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLContext;

import static org.junit.Assert.assertArrayEquals;

public class TestSSLFactory {
    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_SSLFactory_createSSLEngine_applies_secure_protocols() throws Exception {
        // Initialize Configuration object to retrieve default SSL protocols
        Configuration conf = new Configuration();
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // Initialize the SSLFactory
        sslFactory.init();

        // Retrieve the SSL engine
        SSLEngine sslEngine = sslFactory.createSSLEngine();

        // Get the expected protocols from the configuration
        String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

        // Assert that the SSL engine's enabled protocols match the configured protocols
        assertArrayEquals("The enabled protocols should match the configured secure protocols.",
                expectedProtocols, sslEngine.getEnabledProtocols());
    }
}