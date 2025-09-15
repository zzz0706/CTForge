package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import javax.net.ssl.SSLEngine;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TestSSLFactory {
    // test code
   
    @Test
    public void testSSLFactoryInitializationWithValidConfiguration() throws Exception {
    
        Configuration conf = new Configuration();
     
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        sslFactory.init();

        // Assertions to ensure SSLFactory initialization
        KeyStoresFactory keyStoresFactory = sslFactory.getKeystoresFactory();
        assertNotNull("KeyStoresFactory should be initialized", keyStoresFactory);

        // Use createSSLEngine() instead of getContext() since getContext() is not a valid method
        SSLEngine sslEngine = sslFactory.createSSLEngine();
        assertNotNull("SSLEngine should be created and initialized", sslEngine);

        // Additional check to ensure valid configuration in sslEngine
        assertTrue("SSLEngine should be in CLIENT mode", sslEngine.getUseClientMode());

        sslFactory.destroy();
    }
}