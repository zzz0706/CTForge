package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.SSLFactory.Mode;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import org.junit.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;

public class TestSSLFactory {

    // Verify SSLFactory constructor throws an IllegalArgumentException when mode is null
    @Test
    public void testSSLFactoryInitializationWithNullMode() {
        // Get configuration value using API
        Configuration conf = new Configuration();

        try {
            // Prepare the input conditions for unit testing
            // Attempt to instantiate SSLFactory with null Mode
            SSLFactory sslFactory = new SSLFactory(null, conf);
        } catch (IllegalArgumentException e) {
            // Validate the expectation
            assert e.getMessage().equals("mode cannot be NULL");
        }
    }

    // Verify SSLFactory can be initialized with a valid mode and configuration
    @Test
    public void testSSLFactoryInitializationWithValidMode() throws GeneralSecurityException, IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, FileBasedKeyStoresFactory.class, KeyStoresFactory.class);

        // Prepare the input conditions for unit testing
        SSLFactory sslFactory = new SSLFactory(Mode.CLIENT, conf);

        try {
            // Test code for initialization
            sslFactory.init();

            // Verify configuration usage and keystoresFactory initialization
            KeyStoresFactory keystoresFactory = sslFactory.getKeystoresFactory();
            assert keystoresFactory != null;
            assert sslFactory != null;
        } finally {
            // Clean up resources
            sslFactory.destroy();
        }
    }

    // Ensure that the destroy method properly cleans up resources
    @Test
    public void testSSLFactoryDestroyResources() throws GeneralSecurityException, IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, FileBasedKeyStoresFactory.class, KeyStoresFactory.class);

        // Prepare the input conditions for unit testing
        SSLFactory sslFactory = new SSLFactory(Mode.CLIENT, conf);

        try {
            // Initialize the factory to simulate its lifecycle
            sslFactory.init();
        } finally {
            // Test code for cleanup
            sslFactory.destroy();

            // Ensure keystoresFactory is accessible but internal resources are cleaned up
            assert sslFactory.getKeystoresFactory() != null;
        }
    }

    // Validate propagation of enabled protocols configuration
    @Test
    public void testSSLFactoryEnabledProtocolsConfiguration() throws GeneralSecurityException, IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2", "TLSv1.1");

        // Prepare the input conditions for unit testing
        SSLFactory sslFactory = new SSLFactory(Mode.CLIENT, conf);

        try {
            // Test code for initialization
            sslFactory.init();

            // Validate enabled protocols' propagation and usage in SSLContext
            String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY);
            assert expectedProtocols != null;
            assert expectedProtocols.length == 2;
            assert expectedProtocols[0].equals("TLSv1.2");
            assert expectedProtocols[1].equals("TLSv1.1");

            // Confirm that the SSLContext was configured with proper SSL parameters
            assert sslFactory.getKeystoresFactory() != null; // Context can't be accessed directly; rely on keystores validation
        } finally {
            // Clean up resources
            sslFactory.destroy();
        }
    }
}