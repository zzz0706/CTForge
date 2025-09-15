package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class MinSessionTimeoutConfigTest {

    @Test
    public void testMinSessionTimeoutValid() throws IOException {
        // 1. Prepare a valid configuration file
        File configFile = File.createTempFile("zoo", ".cfg");
        configFile.deleteOnExit();
        Properties props = new Properties();
        props.setProperty("tickTime", "2000");
        props.setProperty("minSessionTimeout", "4000");
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, null);
        }

        // 2. Load configuration via QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parse(configFile.getAbsolutePath());
        } catch (QuorumPeerConfig.ConfigException e) {
            fail("ConfigException should not be thrown for valid configuration");
        }

        // 3. Validate that minSessionTimeout is exactly 2 * tickTime when explicitly set
        assertEquals("minSessionTimeout should be 4000", 4000, config.getMinSessionTimeout());

        // 4. Clean up
        configFile.delete();
    }

    @Test
    public void testMinSessionTimeoutDefault() throws IOException {
        // 1. Prepare a configuration file without minSessionTimeout
        File configFile = File.createTempFile("zoo", ".cfg");
        configFile.deleteOnExit();
        Properties props = new Properties();
        props.setProperty("tickTime", "3000");
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, null);
        }

        // 2. Load configuration via QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parse(configFile.getAbsolutePath());
        } catch (QuorumPeerConfig.ConfigException e) {
            fail("ConfigException should not be thrown for valid configuration");
        }

        // 3. Validate that minSessionTimeout defaults to 2 * tickTime
        assertEquals("minSessionTimeout should default to 2 * tickTime", 6000, config.getMinSessionTimeout());

        // 4. Clean up
        configFile.delete();
    }

    @Test
    public void testMinSessionTimeoutNegativeFallback() throws IOException {
        // 1. Prepare a configuration file with minSessionTimeout = -1
        File configFile = File.createTempFile("zoo", ".cfg");
        configFile.deleteOnExit();
        Properties props = new Properties();
        props.setProperty("tickTime", "1500");
        props.setProperty("minSessionTimeout", "-1");
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, null);
        }

        // 2. Load configuration via QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parse(configFile.getAbsolutePath());
        } catch (QuorumPeerConfig.ConfigException e) {
            fail("ConfigException should not be thrown for valid configuration");
        }

        // 3. Validate that minSessionTimeout falls back to 2 * tickTime
        assertEquals("minSessionTimeout should fallback to 2 * tickTime", 3000, config.getMinSessionTimeout());

        // 4. Clean up
        configFile.delete();
    }

    @Test
    public void testMinSessionTimeoutLessThanTickTime() throws IOException {
        // 1. Prepare a configuration file with minSessionTimeout < tickTime
        File configFile = File.createTempFile("zoo", ".cfg");
        configFile.deleteOnExit();
        Properties props = new Properties();
        props.setProperty("tickTime", "5000");
        props.setProperty("minSessionTimeout", "1000");
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, null);
        }

        // 2. Load configuration via QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parse(configFile.getAbsolutePath());
        } catch (QuorumPeerConfig.ConfigException e) {
            fail("ConfigException should not be thrown for valid configuration");
        }

        // 3. Validate that minSessionTimeout is accepted even if < tickTime (no explicit constraint)
        assertEquals("minSessionTimeout should be accepted as-is", 1000, config.getMinSessionTimeout());

        // 4. Clean up
        configFile.delete();
    }
}