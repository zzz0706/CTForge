package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory.Mode;
import org.junit.Test;

public class SSLFactoryConfigurationTest {

    @Test(expected = RuntimeException.class)
    public void testInvalidKeyStoresFactoryClassThrowsConfigurationException() {
        // 1. Create Configuration and inject invalid class
        Configuration conf = new Configuration();
        conf.setClass(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY,
                      Object.class,
                      KeyStoresFactory.class);

        // 2. Attempt to construct SSLFactory — should throw
        new SSLFactory(Mode.CLIENT, conf);
    }
}