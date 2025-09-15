package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalSessionsEnabledConfigTest {

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

    @Test
    public void testLocalSessionsEnabledValidTrue() throws IOException, ConfigException {
        // 1. Prepare the test conditions
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=/tmp/zookeeper\n");
            writer.write("localSessionsEnabled=true\n");
        }
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(configFile.getAbsolutePath());

        // 2. test code
        boolean value = config.localSessionsEnabled;
        assertTrue("localSessionsEnabled should be true", value);
    }

    @Test
    public void testLocalSessionsEnabledValidFalse() throws IOException, ConfigException {
        // 1. prepare the test conditions
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=/tmp/zookeeper\n");
            writer.write("localSessionsEnabled=false\n");
        }
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(configFile.getAbsolutePath());

        // 3. test code
        boolean value = config.localSessionsEnabled;
        assertFalse("localSessionsEnabled should be false", value);
    }

    @Test
    public void testLocalSessionsEnabledInvalidValue() throws IOException, ConfigException {
        // 1. prepare the test conditions
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=/tmp/zookeeper\n");
            writer.write("localSessionsEnabled=maybe\n");
        }
        QuorumPeerConfig config = new QuorumPeerConfig();

        // 2. test code – parsing should not throw ConfigException for invalid boolean
        config.parse(configFile.getAbsolutePath());
        boolean value = config.localSessionsEnabled;
        assertFalse("localSessionsEnabled should default to false for invalid value", value);
    }

    @Test
    public void testLocalSessionsEnabledDefaultFalse() throws IOException, ConfigException {
        // 1. prepare the test conditions
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("dataDir=/tmp/zookeeper\n");
            writer.write("# no localSessionsEnabled key\n");
        }
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(configFile.getAbsolutePath());

        // 3. test code
        boolean value = config.localSessionsEnabled;
        assertFalse("localSessionsEnabled should default to false", value);
    }
}