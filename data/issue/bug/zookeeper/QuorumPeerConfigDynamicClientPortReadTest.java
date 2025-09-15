package org.apache.zookeeper.server.quorum;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.net.InetSocketAddress;

//ZOOKEEPER-2006
public class QuorumPeerConfigDynamicClientPortReadTest {
    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
 * 
    public void testClientPortParsedFromConfig() throws Exception {
        // 1. Load properties from static config file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // 2. Parse properties using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 3. Get clientPortAddress (could be from static or dynamic config)
        InetSocketAddress clientAddr = config.getClientPortAddress();

        // 4. Assert that clientPortAddress is not null and port is a valid port (>0)
        assertNotNull("ClientPortAddress should be parsed from config", clientAddr);
        int port = clientAddr.getPort();
        assertTrue("Client port should be > 0", port > 0);

        System.out.println("Parsed client port from config: " + port);
    }
}
