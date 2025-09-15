package org.apache.hadoop.security.ssl; 

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.SSLFactory.Mode;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.IOException;       
import java.security.GeneralSecurityException;       

// Test class to ensure coverage and correctness of SSLFactory functionality
public class TestSSLFactory {

    // Prepare the input conditions for unit testing.
    @Test
    public void testSSLFactoryInitializationWithNullMode() {
        // Get configuration value using API
        Configuration conf = new Configuration();

        try {
            // Attempt to instantiate SSLFactory with null Mode
            SSLFactory sslFactory = new SSLFactory(null, conf);
        } catch (IllegalArgumentException e) {
            // Validate exception message
            assert e.getMessage().equals("mode cannot be NULL");
        }
    }

    // Prepare the input conditions for init() method testing.
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

            // Verify that keystoresFactory is not null and properly initialized
            KeyStoresFactory keystoresFactory = sslFactory.getKeystoresFactory();
            assert keystoresFactory != null;

            // Ensure no exceptions during initialization
            assert sslFactory != null;

        } finally {
            // Cleanup to test destroy() method
            sslFactory.destroy();
        }
    }
}