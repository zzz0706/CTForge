package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;

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
}