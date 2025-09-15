package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.*;

public class SecureClientPortAddressConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test case to validate the secureClientPortAddress configuration.
     * This test ensures the secureClientPortAddress configuration value satisfies constraints and dependencies.
     */
    @Test
    public void testSecureClientPortAddressConfigValidity() {
        try {
            // Step 1: Load configuration from the file
            Properties props = new Properties();
            try (InputStream inputStream = new FileInputStream(CONFIG_PATH)) {
                props.load(inputStream);
            }

            // Step 2: Parse the configuration into QuorumPeerConfig
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Step 3: Obtain the secureClientPortAddress using the appropriate API
            InetSocketAddress secureClientPortAddress = config.getSecureClientPortAddress();

            // Step 4: Validate the secureClientPortAddress configuration
            // 1. Ensure the value is not null if specified
            if (secureClientPortAddress != null) {
                // 2. Validate the IP address format
                String hostName = secureClientPortAddress.getHostName();
                assertTrue("Invalid secureClientPortAddress: hostname is invalid",
                        isValidHostnameOrIPAddress(hostName));

                // 3. Validate the port number range (valid port range is 0-65535)
                int port = secureClientPortAddress.getPort();
                assertTrue("Invalid secureClientPortAddress: port out of range",
                        port >= 0 && port <= 65535);

                // 4. Check any additional dependencies (e.g., non-conflicting ports, etc.)
                InetSocketAddress clientPortAddress = config.getClientPortAddress();
                if (clientPortAddress != null && clientPortAddress.equals(secureClientPortAddress)) {
                    fail("secureClientPortAddress conflicts with clientPortAddress");
                }
            }

        } catch (IOException e) {
            fail("Failed to read or parse configuration file: " + e.getMessage());
        } catch (Exception e) {
            fail("Unexpected exception during test execution: " + e.getMessage());
        }
    }

    /**
     * Helper method to validate hostnames and IP addresses.
     *
     * @param hostnameOrIPAddress The hostname or IP address to validate.
     * @return true if valid, false otherwise.
     */
    private boolean isValidHostnameOrIPAddress(String hostnameOrIPAddress) {
        // Simplified validation logic
        // For IP addresses: Check whether it's a valid IPv4 or IPv6 address
        // For hostnames: Check whether it matches valid domain name patterns
        return hostnameOrIPAddress.matches(
                "^(?!-)[a-zA-Z0-9-.]{1,253}(?<!-)$") // Domain name validation
                || hostnameOrIPAddress.matches(
                "^([0-9]{1,3}\\.){3}[0-9]{1,3}$"); // IPv4 validation
    }
}