package org.apache.hadoop.security.ssl;

import static org.junit.Assert.assertArrayEquals;

import java.lang.reflect.Field;

import javax.net.ssl.SSLEngine;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SSLFactoryConfigurationTest {

    @Test
    public void testNullValueFallsBackToDefaultProtocols() throws Exception {
        // 1. Create a Configuration instance without touching the key
        Configuration conf = new Configuration();

        // 2. Compute expected value via Configuration#getStrings
        String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY,
                                                     SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

        // 3. Instantiate SSLFactory in CLIENT mode
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
        sslFactory.init();

        // 4. Read the enabledProtocols field via reflection
        Field enabledProtocolsField = SSLFactory.class.getDeclaredField("enabledProtocols");
        enabledProtocolsField.setAccessible(true);
        String[] actualFieldProtocols = (String[]) enabledProtocolsField.get(sslFactory);

        // 5. Create an SSLEngine and read its enabled protocols
        SSLEngine sslEngine = sslFactory.createSSLEngine();
        String[] actualEngineProtocols = sslEngine.getEnabledProtocols();

        // 6. Assertions
        assertArrayEquals("Internal field should contain default protocols",
                          expectedProtocols, actualFieldProtocols);
        assertArrayEquals("SSLEngine should contain default protocols",
                          expectedProtocols, actualEngineProtocols);
    }
}