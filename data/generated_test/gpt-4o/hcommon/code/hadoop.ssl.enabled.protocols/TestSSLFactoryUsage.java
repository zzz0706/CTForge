package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;

import static org.junit.Assert.assertArrayEquals;

public class TestSSLFactoryUsage {

    // Access configuration value using API and prepare input conditions for unit testing

    @Test
    public void test_SSLFactory_constructor_applies_configuration() throws Exception {
        // Arrange: Create a Configuration object with desired secure protocols
        Configuration conf = new Configuration();
        conf.setStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2", "TLSv1.3");

        // Act: Create SSLFactory instance with the configuration
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // Assert: Verify that enabled protocols in SSLFactory match the configuration
        String[] actualProtocols = (String[]) getPrivateField(sslFactory, "enabledProtocols");
        String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

        java.util.Arrays.sort(actualProtocols);
        java.util.Arrays.sort(expectedProtocols);

        assertArrayEquals("Enabled protocols should match the configuration.", expectedProtocols, actualProtocols);
    }

    @Test
    public void test_SSLFactory_init_configures_context_protocols() throws Exception {
        // Arrange: Configure secure protocols in Configuration
        Configuration conf = new Configuration();
        conf.setStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2", "TLSv1.3");

        // Act: Initialize SSLFactory and access context's SSL parameters
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
        sslFactory.init();

        SSLContext sslContext = (SSLContext) getPrivateField(sslFactory, "context");
        String[] enabledProtocols = sslContext.getDefaultSSLParameters().getProtocols();
        String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

        java.util.Arrays.sort(enabledProtocols);
        java.util.Arrays.sort(expectedProtocols);

        // Assert: Verify configured protocols are applied to context
        assertArrayEquals("Context protocols should match configuration.", expectedProtocols, enabledProtocols);
    }

    @Test
    public void test_SSLFactory_createSSLEngine_applies_configured_protocols() throws IOException, GeneralSecurityException {
        // Arrange: Set up secure protocols in Configuration
        Configuration conf = new Configuration();
        conf.setStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2", "TLSv1.3");

        // Act: Initialize SSLFactory and create SSLEngine instance
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
        sslFactory.init();
        SSLEngine sslEngine = sslFactory.createSSLEngine();

        // Assert: Verify enabled protocols on SSLEngine match the configuration
        String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);
        assertArrayEquals("SSLEngine enabled protocols should match configuration.", expectedProtocols, sslEngine.getEnabledProtocols());
    }

    /**
     * Helper method to use reflection to access private fields.
     */
    private Object getPrivateField(Object obj, String fieldName) throws Exception {
        java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }
}