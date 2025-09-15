package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;

import static org.junit.Assert.assertArrayEquals;

public class TestSSLFactory {

    // Helper method to use reflection to access private fields
    private Object getPrivateField(Object obj, String fieldName) throws Exception {
        java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }

    @Test
    public void test_SSLFactory_createSSLEngine_applies_secure_protocols() throws GeneralSecurityException, IOException {
        // 1. Configure the `Configuration` object with desired protocols
        Configuration conf = new Configuration();
        conf.setStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2", "TLSv1.3");

        // 2. Create SSLFactory instance and initialize it
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
        sslFactory.init();

        // 3. Create an SSLEngine to verify enabled protocols
        SSLEngine sslEngine = sslFactory.createSSLEngine();

        // 4. Retrieve expected values from configuration
        String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

        // 5. Compare the enabled protocols on the SSLEngine with the expected values
        assertArrayEquals("The enabled protocols should match the configured secure protocols.",
                expectedProtocols, sslEngine.getEnabledProtocols());
    }

    @Test
    public void test_SSLFactory_init_sets_context_protocols() throws Exception {
        // 1. Configure the Configuration object with desired protocols
        Configuration conf = new Configuration();
        conf.setStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2", "TLSv1.3");

        // 2. Initialize SSLFactory in CLIENT mode
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
        sslFactory.init();

        // 3. Reflectively access the private field `context`
        SSLContext sslContext = (SSLContext) getPrivateField(sslFactory, "context");

        // 4. Verify the default SSL parameters contain the correct protocols
        String[] enabledProtocols = sslContext.getDefaultSSLParameters().getProtocols();
        String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

        // Sort protocols before comparison to prevent mismatch due to order differences
        java.util.Arrays.sort(enabledProtocols);
        java.util.Arrays.sort(expectedProtocols);

        assertArrayEquals("The context's default SSL parameters protocols should match the configured secure protocols.",
                expectedProtocols, enabledProtocols);
    }

    @Test
    public void test_SSLFactory_constructor_applies_configuration() throws Exception {
        // 1. Configure the Configuration object with the desired protocols
        Configuration conf = new Configuration();
        conf.setStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2", "TLSv1.3");

        // 2. Create an SSLFactory instance using the given configuration
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // 3. Use reflection to verify `enabledProtocols`
        String[] actualProtocols = (String[]) getPrivateField(sslFactory, "enabledProtocols");

        // 4. Retrieve expected protocols from configuration
        String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

        // Sort protocols to ensure comparison does not fail due to order
        java.util.Arrays.sort(actualProtocols);
        java.util.Arrays.sort(expectedProtocols);

        assertArrayEquals("The SSLFactory's enabledProtocols should match the configuration.",
                expectedProtocols, actualProtocols);
    }
}