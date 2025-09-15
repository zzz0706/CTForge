package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

public class SecureClientPortConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testSecureClientPortConfiguration() throws IOException {
        // Step 1: Load configuration from file
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse the configuration using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (QuorumPeerConfig.ConfigException e) {
            fail("Failed to parse configuration properties: " + e.getMessage());
        }

        // Step 3: Validate the secureClientPort configuration and mixed-mode dependencies
        InetSocketAddress secureClientPortAddress = config.getSecureClientPortAddress();
        if (secureClientPortAddress != null) {
            int secureClientPort = secureClientPortAddress.getPort();

            // Validate secureClientPort constraints
            assertTrue("secureClientPort must be a positive number within valid port ranges.", secureClientPort > 0 && secureClientPort <= 65535);

            // Check mixed-mode configuration dependency
            InetSocketAddress clientPortAddress = config.getClientPortAddress();
            assertNotNull("Mixed-mode operation requires both clientPort and secureClientPort to be specified.", clientPortAddress);
        } else {
            // If secureClientPort is omitted, validate mixed-mode dependencies
            InetSocketAddress clientPortAddress = config.getClientPortAddress();
            assertNull("Mixed-mode operation requires both clientPort and secureClientPort to be omitted.", clientPortAddress);
        }
    }
}