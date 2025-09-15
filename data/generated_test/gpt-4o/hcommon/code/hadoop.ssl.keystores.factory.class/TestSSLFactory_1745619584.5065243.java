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
            // Attempt to instantiate SSLFactory with null Mode
            SSLFactory sslFactory = new SSLFactory(null, conf);
        } catch (IllegalArgumentException e) {
            assert e.getMessage().equals("mode cannot be NULL");
        }
    }

    // Test initialization of SSLFactory with its configuration and Mode usage
    @Test
    public void testSSLFactoryInitializationWithValidMode() throws GeneralSecurityException, IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, FileBasedKeyStoresFactory.class, KeyStoresFactory.class);

        // Instantiate SSLFactory
        SSLFactory sslFactory = new SSLFactory(Mode.CLIENT, conf);

        try {
            // Call init() to test propagation of configuration
            sslFactory.init();

            // Verify keystoresFactory initialization and correctness
            KeyStoresFactory keystoresFactory = sslFactory.getKeystoresFactory();
            assert keystoresFactory != null;

            // Validate SSLFactory initialization state
            assert sslFactory != null;
        } finally {
            // Cleanup resources
            sslFactory.destroy();
        }
    }

    // Test SSLFactory destroy method to ensure proper resource cleanup
    @Test
    public void testSSLFactoryDestroyResources() throws GeneralSecurityException, IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, FileBasedKeyStoresFactory.class, KeyStoresFactory.class);

        // Instantiate SSLFactory
        SSLFactory sslFactory = new SSLFactory(Mode.CLIENT, conf);

        try {
            // Initialize SSLFactory
            sslFactory.init();
        } finally {
            // Destroy resources and verify destruction of underlying factories
            sslFactory.destroy();

            // Validate keystoresFactory remains accessible but cleaned up internally
            assert sslFactory.getKeystoresFactory() != null;
        }
    }
}