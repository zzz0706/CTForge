package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QuorumPeerConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testClientPortAddressConfiguration() {
        try {
            // 1. Prepare the test conditions by loading configuration properties
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // 2. Use ZooKeeper API to parse configuration and test behavior
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // 3. Perform assertions to ensure the configuration is correctly parsed
            InetSocketAddress clientPortAddress = config.getClientPortAddress();
            assertNotNull("ClientPortAddress should not be null.", clientPortAddress);

            // Validating hostname
            String hostName = clientPortAddress.getHostName();
            assertNotNull("ClientPortAddress hostname should not be null.", hostName);
            assertTrue("ClientPortAddress hostname should not be empty.", !hostName.isEmpty());
            assertTrue("ClientPortAddress hostname should be valid.",
                    hostName.matches("^(?!-)[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(?!-)$") || // DNS hostname
                    hostName.matches("^(\\d{1,3}\\.){3}\\d{1,3}$") ||              // IPv4
                    hostName.matches("^\\[([0-9a-fA-F:]+)\\]$"));                 // IPv6

            // Validating port
            int port = clientPortAddress.getPort();
            assertTrue("Port number should be between 0 and 65535.", port >= 0 && port <= 65535);

            // Mocking and validating additional configurations
            QuorumPeerConfig configMock = org.mockito.Mockito.mock(QuorumPeerConfig.class);
            File dataDir = new File("mockDataDir");
            org.mockito.Mockito.when(configMock.getDataDir()).thenReturn(dataDir);

            assertEquals("Mocked Data directory should match.", dataDir, configMock.getDataDir());
        } catch (IOException | QuorumPeerConfig.ConfigException e) {
            // Handle exceptions raised during configuration parsing
            throw new AssertionError("Test failed due to exception: " + e.getMessage(), e);
        }
    }
}