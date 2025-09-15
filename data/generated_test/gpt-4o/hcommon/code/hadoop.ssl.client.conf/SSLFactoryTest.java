package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;

public class SSLFactoryTest {

    // Prepare the input conditions for unit testing.
    @Test
    public void testSSLFactoryConstructorNullMode() {
        // Get configuration values using API and initialize a Configuration instance
        Configuration conf = new Configuration();

        try {
            // Attempt to instantiate SSLFactory with null mode
            new SSLFactory(null, conf);
        } catch (IllegalArgumentException e) {
            // Assert that the exception message indicates the mode cannot be null
            assert e.getMessage().contains("mode cannot be NULL");
        }
    }
}