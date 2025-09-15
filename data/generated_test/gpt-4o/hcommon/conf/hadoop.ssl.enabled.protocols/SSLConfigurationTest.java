package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class SSLConfigurationTest {

    private static final String SSL_ENABLED_KEY = "hadoop.ssl.enabled";
    private static final String SSL_ENABLED_PROTOCOLS_KEY = "hadoop.ssl.enabled.protocols";
    private static final String DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1,SSLv2Hello,TLSv1.1,TLSv1.2";

    /**
     * Validates SSL-related configurations read from a Hadoop configuration file.
     */
    @Test
    public void testSSLConfigurations() {
        Configuration conf = new Configuration();

        // Step 1: Validate presence and correctness of "hadoop.ssl.enabled".
        boolean sslEnabled = conf.getBoolean(SSL_ENABLED_KEY, false);
        if (!sslEnabled) {
            System.out.println("SSL is disabled, skipping protocols validation.");
            return; // No further validation required when SSL is not enabled.
        }

        // Step 2: Validate "hadoop.ssl.enabled.protocols" configuration.
        String[] enabledProtocols = conf.getStrings(SSL_ENABLED_PROTOCOLS_KEY, DEFAULT_SSL_ENABLED_PROTOCOLS.split(","));
        assertNotNull("Enabled protocols should not be null if SSL is enabled.", enabledProtocols);

        // Ensure no unsupported protocols are specified.
        for (String protocol : enabledProtocols) {
            assertTrue("Unsupported SSL protocol: " + protocol, isSupportedProtocol(protocol));
        }
    }

    /**
     * Checks if the given protocol is a supported SSL protocol.
     *
     * @param protocol Protocol string.
     * @return True if the protocol is supported, otherwise false.
     */
    private boolean isSupportedProtocol(String protocol) {
        switch (protocol) {
            case "TLSv1":
            case "SSLv2Hello":
            case "TLSv1.1":
            case "TLSv1.2":
                return true;
            default:
                return false;
        }
    }
}