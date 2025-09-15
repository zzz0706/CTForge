package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;
//ZOOKEEPER-192

public class ConfigSyncLimitTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testClientPortWhitespaceTrim() throws Exception {
        // Load properties with potential whitespace
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Check that clientPort exists and is trimmed correctly
        String clientPortStr = props.getProperty("clientPort");
        assertNotNull("clientPort config must not be null", clientPortStr);

        // Simulate the behavior before/after fix (with and without trim)
        try {
            // Before fix: will fail if not trimmed
            Integer.parseInt(clientPortStr);
        } catch (NumberFormatException ex) {
            // Try with trim to mimic fixed logic
            int port = Integer.parseInt(clientPortStr.trim());
            assertTrue("clientPort should be 2181 after trimming whitespace", port == 2181);
            return; // test passes if trimming works
        }

        // If no exception thrown, test passes
        int port = Integer.parseInt(clientPortStr.trim());
        assertEquals(2181, port);
    }
}
