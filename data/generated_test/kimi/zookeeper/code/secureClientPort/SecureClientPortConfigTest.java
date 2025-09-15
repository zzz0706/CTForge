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

public class SecureClientPortConfigTest {

    private File configFile;
    private File dataDir;

    @Before
    public void setUp() throws IOException {
        configFile = File.createTempFile("zoo", ".cfg");
        configFile.deleteOnExit();
        dataDir = File.createTempFile("data", "");
        dataDir.delete();
        dataDir.mkdir();
        dataDir.deleteOnExit();
    }

    @After
    public void tearDown() {
        if (configFile != null) {
            configFile.delete();
        }
        if (dataDir != null) {
            for (File f : dataDir.listFiles()) {
                f.delete();
            }
            dataDir.delete();
        }
    }

    @Test
    public void testValidSecureClientPort() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", dataDir.getAbsolutePath());
        props.setProperty("secureClientPort", "2281");
        writeConfig(props);

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(configFile.getAbsolutePath());

        assertNotNull("secureClientPort should be parsed", config.getSecureClientPortAddress());
        assertEquals("secureClientPort value", 2281, config.getSecureClientPortAddress().getPort());
    }

    @Test
    public void testInvalidSecureClientPortNegative() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", dataDir.getAbsolutePath());
        props.setProperty("secureClientPort", "-1");
        writeConfig(props);

        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parse(configFile.getAbsolutePath());
            fail("Expected ConfigException for negative port");
        } catch (QuorumPeerConfig.ConfigException expected) {
            assertTrue(expected.getCause() instanceof IllegalArgumentException);
            assertTrue(expected.getCause().getMessage().contains("port out of range"));
        }
    }

    @Test
    public void testInvalidSecureClientPortOutOfRange() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", dataDir.getAbsolutePath());
        props.setProperty("secureClientPort", "65536");
        writeConfig(props);

        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parse(configFile.getAbsolutePath());
            fail("Expected ConfigException for port > 65535");
        } catch (QuorumPeerConfig.ConfigException expected) {
            assertTrue(expected.getCause() instanceof IllegalArgumentException);
            assertTrue(expected.getCause().getMessage().contains("port out of range"));
        }
    }

    @Test
    public void testSecureClientPortNotSet() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", dataDir.getAbsolutePath());
        // omit secureClientPort
        writeConfig(props);

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(configFile.getAbsolutePath());

        assertNull("secureClientPort should be null when not configured", config.getSecureClientPortAddress());
    }

    @Test
    public void testSecureClientPortWithClientPort() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", dataDir.getAbsolutePath());
        props.setProperty("clientPort", "2181");
        props.setProperty("secureClientPort", "2281");
        writeConfig(props);

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(configFile.getAbsolutePath());

        assertNotNull("clientPort should be parsed", config.getClientPortAddress());
        assertEquals("clientPort value", 2181, config.getClientPortAddress().getPort());
        assertNotNull("secureClientPort should be parsed", config.getSecureClientPortAddress());
        assertEquals("secureClientPort value", 2281, config.getSecureClientPortAddress().getPort());
    }

    private void writeConfig(Properties props) throws IOException {
        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, null);
        }
    }
}