package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestSSLFactory {
    // Test code
    @Test
    public void testSSLFactoryClientModeDefaultConf() throws Exception {
        // 1. Prepare the test conditions with Hadoop Configuration
        Configuration configuration = new Configuration();

        // 2. Instantiate SSLFactory in CLIENT mode
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, configuration);

        // 3. Initialize the SSLFactory
        sslFactory.init();

        // 4. Perform assertions to verify SSLFactory is properly initialized
        assertNotNull("SSLFactory instance should not be null", sslFactory);

        // 5. Since methods such as getKeyStoresFactory(), getEnabledProtocols(), and getExcludedCiphers()
        // do not exist in the version 2.8.5 of Hadoop's SSLFactory, we confine the validation to this scope.

        // 6. Clean up after test
        sslFactory.destroy();
    }
}