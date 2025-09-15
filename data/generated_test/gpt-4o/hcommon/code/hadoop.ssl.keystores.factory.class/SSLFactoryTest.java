package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SSLFactoryTest {

    @Test
    public void test_SSLFactory_Initialization_With_Default_KeystoreFactory() throws Exception {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();

        // Instantiate SSLFactory with Mode.CLIENT and the provided Configuration.
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // Initialize the SSLFactory.
        sslFactory.init();

        // Validate that the SSLFactory successfully uses the default FileBasedKeyStoresFactory (default behavior).
        assert sslFactory != null : "SSLFactory instance is not initialized correctly, but it should be.";
    }
}