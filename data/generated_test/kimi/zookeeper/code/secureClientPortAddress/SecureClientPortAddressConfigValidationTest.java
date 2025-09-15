package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileWriter;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SecureClientPortAddressConfigValidationTest {

    private File configFile;
    private File dataDir;
    private QuorumPeerConfig config;

    @Before
    public void setUp() throws Exception {
        configFile = File.createTempFile("zoo", ".cfg");
        configFile.deleteOnExit();
        dataDir = File.createTempFile("data", "");
        dataDir.delete();
        dataDir.mkdir();
        dataDir.deleteOnExit();
        config = new QuorumPeerConfig();
    }

    @After
    public void tearDown() {
        if (configFile != null) {
            configFile.delete();
        }
        if (dataDir != null) {
            dataDir.delete();
        }
    }

    @Test
    public void testValidSecureClientPortAddress() throws Exception {
        // Prepare a valid secureClientPortAddress
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=" + dataDir.getAbsolutePath() + "\n");
            writer.write("secureClientPort=2281\n");
            writer.write("secureClientPortAddress=0.0.0.0\n");
            writer.write("server.1=localhost:2888:3888\n");
        }
        config.parse(configFile.getAbsolutePath());

        InetSocketAddress addr = config.getSecureClientPortAddress();
        assertTrue("secureClientPortAddress should be valid",
                   addr != null && addr.getPort() == 2281);
    }

    @Test
    public void testInvalidSecureClientPortAddressFormat() throws Exception {
        // Provide malformed address
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=" + dataDir.getAbsolutePath() + "\n");
            writer.write("secureClientPort=2281\n");
            writer.write("secureClientPortAddress=999.999.999.999\n");
            writer.write("server.1=localhost:2888:3888\n");
        }
        boolean threw = false;
        try {
            config.parse(configFile.getAbsolutePath());
        } catch (QuorumPeerConfig.ConfigException e) {
            threw = true;
        }
        assertTrue("Should throw ConfigException for malformed address", threw);
    }

    @Test
    public void testSecureClientPortAddressWithoutPort() throws Exception {
        // secureClientPortAddress present but secureClientPort missing
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=" + dataDir.getAbsolutePath() + "\n");
            writer.write("secureClientPortAddress=127.0.0.1\n");
            writer.write("server.1=localhost:2888:3888\n");
        }
        boolean threw = false;
        try {
            config.parse(configFile.getAbsolutePath());
        } catch (QuorumPeerConfig.ConfigException e) {
            threw = true;
        }
        assertTrue("Should throw ConfigException when secureClientPort is missing", threw);
    }

    @Test
    public void testSecureClientPortAddressWithZeroPort() throws Exception {
        // secureClientPort set to 0
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=" + dataDir.getAbsolutePath() + "\n");
            writer.write("secureClientPort=0\n");
            writer.write("server.1=localhost:2888:3888\n");
        }
        config.parse(configFile.getAbsolutePath());
        assertNull("secureClientPortAddress should be null when port is 0",
                   config.getSecureClientPortAddress());
    }

    @Test
    public void testSecureClientPortAddressWithNegativePort() throws Exception {
        // Negative port
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=" + dataDir.getAbsolutePath() + "\n");
            writer.write("secureClientPort=-1\n");
            writer.write("server.1=localhost:2888:3888\n");
        }
        boolean threw = false;
        try {
            config.parse(configFile.getAbsolutePath());
        } catch (QuorumPeerConfig.ConfigException e) {
            threw = true;
        }
        assertTrue("Should throw ConfigException for negative port", threw);
    }

    @Test
    public void testSecureClientPortAddressWithOutOfRangePort() throws Exception {
        // Port > 65535
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=" + dataDir.getAbsolutePath() + "\n");
            writer.write("secureClientPort=70000\n");
            writer.write("server.1=localhost:2888:3888\n");
        }
        boolean threw = false;
        try {
            config.parse(configFile.getAbsolutePath());
        } catch (QuorumPeerConfig.ConfigException e) {
            threw = true;
        }
        assertTrue("Should throw ConfigException for port > 65535", threw);
    }
}