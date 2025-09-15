package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import javax.net.ssl.SSLEngine;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TestSSLFactory {
    // get congiguration value using API
    @Test
    public void testSSLFactoryInitializationWithValidConfiguration() throws Exception {
        // Prepare the input conditions for unit testing
        Configuration conf = new Configuration();
        conf.set(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, "org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory");

        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // Test code
        sslFactory.init();

        KeyStoresFactory keyStoresFactory = sslFactory.getKeystoresFactory();
        assertNotNull("KeyStoresFactory should be initialized", keyStoresFactory);

        SSLEngine sslEngine = sslFactory.createSSLEngine();
        assertNotNull("SSLEngine should be created and initialized", sslEngine);
        assertTrue("SSLEngine should be in CLIENT mode", sslEngine.getUseClientMode());

        // Clean up after the test
        sslFactory.destroy();
    }
}