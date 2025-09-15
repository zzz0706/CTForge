package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class ClientPortConfigValidationTest {

    private File configFile;

    @Before
    public void setUp() throws IOException {
        configFile = File.createTempFile("zoo", ".cfg");
        configFile.deleteOnExit();
    }

    @After
    public void tearDown() {
        if (configFile != null) {
            configFile.delete();
        }
    }

    private QuorumPeerConfig loadConfig(Properties props) throws IOException, org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException {
        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, null);
        }
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(configFile.getAbsolutePath());
        return config;
    }

    @Test
    public void testClientPortValidRange() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");

        QuorumPeerConfig config = loadConfig(props);
        int clientPort = config.getClientPortAddress().getPort();
        assertTrue("clientPort must be between 1 and 65535", clientPort > 0 && clientPort <= 65535);
    }

    @Test
    public void testClientPortZeroInvalid() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "0");

        QuorumPeerConfig config = loadConfig(props);
        assertNull("clientPort=0 should result in null address", config.getClientPortAddress());
    }

    @Test
    public void testClientPortNegativeInvalid() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "-1");

        try {
            loadConfig(props);
            fail("Expected ConfigException for clientPort=-1");
        } catch (org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException e) {
            // expected
        }
    }

    @Test
    public void testClientPortOutOfRangeInvalid() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "65536");

        try {
            loadConfig(props);
            fail("Expected ConfigException for clientPort=65536");
        } catch (org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException e) {
            // expected
        }
    }

    @Test
    public void testClientPortMissing() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");

        QuorumPeerConfig config = loadConfig(props);
        assertNull("Missing clientPort should result in null address", config.getClientPortAddress());
    }

    @Test
    public void testClientPortNonNumeric() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "abc");

        try {
            loadConfig(props);
            fail("Expected ConfigException for non-numeric clientPort");
        } catch (org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException e) {
            // expected
        }
    }
}