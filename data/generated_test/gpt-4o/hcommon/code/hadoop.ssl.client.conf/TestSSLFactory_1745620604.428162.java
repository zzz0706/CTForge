package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import static org.junit.Assert.*;

public class TestSSLFactory {

    @Test
    public void testSSLEnabledProtocolsValidation() throws Exception {
        // 1. Use API to fetch configuration values (avoid hardcoding)
        // Prepare the test configuration with no explicit `ssl.enabled.protocols`
        Configuration defaultConf = new Configuration();
        SSLFactory defaultSSLFactory = new SSLFactory(SSLFactory.Mode.CLIENT, defaultConf);
        defaultSSLFactory.init();

        try {
            SSLSocketFactory defaultSocketFactory = (SSLSocketFactory) defaultSSLFactory.createSSLSocketFactory();

            // Verify default enabled protocols
            SSLSocket defaultSocket = (SSLSocket) defaultSocketFactory.createSocket();
            String[] enabledProtocolsDefault = defaultSocket.getEnabledProtocols();

            assertNotNull("Default enabled protocols should not be null", enabledProtocolsDefault);
            assertTrue("Default enabled protocols should not be empty", enabledProtocolsDefault.length > 0);

            // 2. Configure specific SSL protocols by setting `ssl.enabled.protocols`
            Configuration updatedConf = new Configuration();
            updatedConf.set("ssl.enabled.protocols", "TLSv1.2");

            SSLFactory updatedSSLFactory = new SSLFactory(SSLFactory.Mode.CLIENT, updatedConf);
            updatedSSLFactory.init();

            try {
                SSLSocketFactory updatedSocketFactory = (SSLSocketFactory) updatedSSLFactory.createSSLSocketFactory();
                SSLSocket updatedSocket = (SSLSocket) updatedSocketFactory.createSocket();
                String[] enabledProtocolsUpdated = updatedSocket.getEnabledProtocols();

                // Verify that only the specified protocol (TLSv1.2) is enabled
                assertNotNull("Updated enabled protocols should not be null", enabledProtocolsUpdated);

                // Since Java may include multiple versions of protocols by default, manually check for the presence of TLSv1.2
                boolean isTLSv1_2Configured = false;
                for (String protocol : enabledProtocolsUpdated) {
                    if ("TLSv1.2".equals(protocol)) {
                        isTLSv1_2Configured = true;
                        break;
                    }
                }
                assertTrue("TLSv1.2 should be one of the enabled protocols", isTLSv1_2Configured);
            } finally {
                // 4. Clean up - destroy the updated SSL factory
                updatedSSLFactory.destroy();
            }
        } finally {
            // 4. Clean up - destroy the default SSL factory
            defaultSSLFactory.destroy();
        }
    }
}