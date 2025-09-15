package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * Unit test for verifying the correctness of clientPort configuration in ZooKeeper 3.5.6.
 */
public class ClientPortConfigurationTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to validate the clientPort configuration obtained from the ZooKeeper configuration file.
     */
    @Test
    public void testClientPortConfiguration() throws Exception {
        // Step 1: Load the properties file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse the configuration using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 3: Retrieve clientPort configuration
        InetSocketAddress clientPortAddress = config.getClientPortAddress();

        // Step 4: Validate clientPort configuration
        assertNotNull("clientPort configuration should not be null", clientPortAddress);

        int clientPort = clientPortAddress.getPort();
        assertTrue("clientPort should be within the valid range (1-65535)", clientPort > 0 && clientPort <= 65535);
    }
}