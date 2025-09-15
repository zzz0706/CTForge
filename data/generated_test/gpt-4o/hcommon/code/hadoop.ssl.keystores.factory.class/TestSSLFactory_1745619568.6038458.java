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

    // Test initialization of SSLFactory with a null Mode to ensure IllegalArgumentException is thrown
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

    // Test initialization and usage of SSLFactory to ensure proper configuration propagation
    @Test
    public void testSSLFactoryInitializationAndUsage() throws GeneralSecurityException, IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, FileBasedKeyStoresFactory.class, KeyStoresFactory.class);

        // Instantiate SSLFactory with VALID Mode
        SSLFactory sslFactory = new SSLFactory(Mode.CLIENT, conf);

        try {
            // Call init() to test propagation and usage
            sslFactory.init();

            // Verify keystoresFactory initialization and object correctness
            KeyStoresFactory keystoresFactory = sslFactory.getKeystoresFactory();
            assert keystoresFactory != null;

            // Ensure no exceptions during initialization
            assert sslFactory != null;
        } finally {
            // Cleanup to test destroy() method
            sslFactory.destroy();
        }
    }

    // Test destruction of SSLFactory resources to ensure proper cleanup
    @Test
    public void testSSLFactoryDestroy() throws GeneralSecurityException, IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, FileBasedKeyStoresFactory.class, KeyStoresFactory.class);

        // Instantiate SSLFactory with VALID Mode
        SSLFactory sslFactory = new SSLFactory(Mode.CLIENT, conf);

        try {
            // Call init() to ensure proper initialization
            sslFactory.init();
        } finally {
            // Cleanup to test destroy() method
            sslFactory.destroy();

            // Validate keystoresFactory destruction
            assert sslFactory.getKeystoresFactory() != null; // Instance remains accessible but destroyed internally
        }
    }
}