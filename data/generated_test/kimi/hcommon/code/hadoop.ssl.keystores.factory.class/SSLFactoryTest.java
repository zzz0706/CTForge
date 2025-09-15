package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SSLFactoryTest {

    @Test
    public void testDefaultKeyStoresFactoryIsUsedWhenNotConfigured() throws Exception {
        // 1. Create a new empty Configuration instance (no conf.set(...))
        Configuration conf = new Configuration();

        // 2. Instantiate SSLFactory with Mode.CLIENT and the empty Configuration
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // 3. Call init()
        sslFactory.init();

        // 4. Call getKeystoresFactory() and check its runtime type
        KeyStoresFactory actualFactory = sslFactory.getKeystoresFactory();
        Class<?> expectedClass = conf.getClass(
                SSLFactory.KEYSTORES_FACTORY_CLASS_KEY,
                FileBasedKeyStoresFactory.class,
                KeyStoresFactory.class);

        assertEquals(expectedClass, actualFactory.getClass());

        // 5. Code after testing: clean up
        sslFactory.destroy();
    }
}