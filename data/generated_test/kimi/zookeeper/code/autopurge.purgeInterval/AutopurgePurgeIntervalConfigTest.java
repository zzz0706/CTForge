package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class AutopurgePurgeIntervalConfigTest {

    private File configFile;
    private QuorumPeerConfig config;

    @Before
    public void setUp() throws IOException {
        configFile = File.createTempFile("zoo", ".cfg");
        configFile.deleteOnExit();
        config = new QuorumPeerConfig();
    }

    @After
    public void tearDown() {
        if (configFile != null) {
            configFile.delete();
        }
    }

    @Test
    public void testValidPositivePurgeInterval() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("dataLogDir", "/tmp/zookeeper/logs");
        props.setProperty("autopurge.snapRetainCount", "3");
        props.setProperty("autopurge.purgeInterval", "1");

        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, "");
        }

        config.parse(configFile.getAbsolutePath());
        assertEquals("Valid positive purgeInterval should be accepted", 1, config.getPurgeInterval());
    }

    @Test
    public void testZeroPurgeIntervalDisablesTask() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("dataLogDir", "/tmp/zookeeper/logs");
        props.setProperty("autopurge.snapRetainCount", "3");
        props.setProperty("autopurge.purgeInterval", "0");

        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, "");
        }

        config.parse(configFile.getAbsolutePath());
        assertEquals("Zero purgeInterval should disable task", 0, config.getPurgeInterval());
    }

    @Test
    public void testNegativePurgeIntervalDisablesTask() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("dataLogDir", "/tmp/zookeeper/logs");
        props.setProperty("autopurge.snapRetainCount", "3");
        props.setProperty("autopurge.purgeInterval", "-1");

        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, "");
        }

        config.parse(configFile.getAbsolutePath());
        assertEquals("Negative purgeInterval should disable task", -1, config.getPurgeInterval());
    }

    @Test
    public void testNonIntegerPurgeIntervalFails() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("dataLogDir", "/tmp/zookeeper/logs");
        props.setProperty("autopurge.snapRetainCount", "3");
        props.setProperty("autopurge.purgeInterval", "1.5");

        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, "");
        }

        try {
            config.parse(configFile.getAbsolutePath());
            fail("Non-integer purgeInterval should cause ConfigException");
        } catch (ConfigException e) {
            // Expected
        }
    }

    @Test
    public void testMissingPurgeIntervalDefaultsToZero() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("dataLogDir", "/tmp/zookeeper/logs");
        props.setProperty("autopurge.snapRetainCount", "3");
        // autopurge.purgeInterval not set

        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, "");
        }

        config.parse(configFile.getAbsolutePath());
        assertEquals("Missing purgeInterval should default to 0", 0, config.getPurgeInterval());
    }

    @Test
    public void testPurgeIntervalWithZeroSnapRetainCount() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("dataLogDir", "/tmp/zookeeper/logs");
        props.setProperty("autopurge.snapRetainCount", "0");
        props.setProperty("autopurge.purgeInterval", "1");

        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, "");
        }

        config.parse(configFile.getAbsolutePath());
        assertEquals("purgeInterval should be accepted even with zero snapRetainCount", 1, config.getPurgeInterval());
    }

    @Test
    public void testLargePurgeIntervalAccepted() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("dataLogDir", "/tmp/zookeeper/logs");
        props.setProperty("autopurge.snapRetainCount", "3");
        props.setProperty("autopurge.purgeInterval", "8760"); // 1 year in hours

        try (FileWriter writer = new FileWriter(configFile)) {
            props.store(writer, "");
        }

        config.parse(configFile.getAbsolutePath());
        assertEquals("Large valid purgeInterval should be accepted", 8760, config.getPurgeInterval());
    }
}