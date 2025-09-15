package org.apache.zookeeper.server.quorum;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

//ZOOKEEPER-209 ZOOKEEPER-188 ZOOKEEPER-2873
public class ServerElectionPortConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testElectionPortConfigured() throws Exception {
        // Load properties from config file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Use QuorumPeerConfig to parse properties (may throw exception for bad config)
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (Exception e) {
            fail("Failed to parse config: " + e.getMessage());
        }

        // Manual election port check (to catch silent misconfig, before ZK3.5+ hardens it)
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("server.")) {
                String value = props.getProperty(key).trim();
                String[] tokens = value.split(":");
                assertTrue(
                    String.format("Config %s is invalid: must specify election port (host:port:electionPort), found: %s", key, value),
                    tokens.length >= 3
                );
                if (tokens.length >= 3) {
                    try {
                        Integer.parseInt(tokens[2]);
                    } catch (NumberFormatException e) {
                        fail(String.format("Config %s: election port '%s' is not a valid integer", key, tokens[2]));
                    }
                }
            }
        }
    }
}
