package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory.Mode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SSLFactoryCustomKeyStoresTest {

    private SSLFactory sslFactory;

    @Test
    public void testCustomKeyStoresFactoryIsInstantiatedWhenConfigured() throws Exception {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Dynamic Expected Value Calculation
        Class<? extends KeyStoresFactory> expectedClass =
            conf.getClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY,
                          TestKeyStoresFactory.class, KeyStoresFactory.class);

        // 3. Prepare the test conditions
        conf.setClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY,
                      TestKeyStoresFactory.class, KeyStoresFactory.class);

        // 4. Invoke the Method Under Test
        sslFactory = new SSLFactory(Mode.SERVER, conf);
        sslFactory.init();

        // 5. Assertions and Verification
        KeyStoresFactory actualFactory = sslFactory.getKeystoresFactory();
        assertNotNull("keystoresFactory should not be null", actualFactory);
        assertTrue("keystoresFactory should be an instance of TestKeyStoresFactory",
                   actualFactory instanceof TestKeyStoresFactory);
    }

    @After
    public void tearDown() {
        if (sslFactory != null) {
            sslFactory.destroy();
        }
    }

    /**
     * Test-only stub implementation of KeyStoresFactory.
     */
    public static class TestKeyStoresFactory implements KeyStoresFactory {
        private Configuration conf;

        @Override
        public void init(SSLFactory.Mode mode) {}

        @Override
        public javax.net.ssl.KeyManager[] getKeyManagers() { return new javax.net.ssl.KeyManager[0]; }

        @Override
        public javax.net.ssl.TrustManager[] getTrustManagers() { return new javax.net.ssl.TrustManager[0]; }

        @Override
        public void destroy() {}

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public Configuration getConf() {
            return conf;
        }
    }
}