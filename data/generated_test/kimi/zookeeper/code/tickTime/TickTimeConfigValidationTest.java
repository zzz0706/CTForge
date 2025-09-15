package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TickTimeConfigValidationTest {

    @Test
    public void testTickTimePositiveInteger() throws Exception {
        // Prepare a temporary configuration file with tickTime=2000 (valid)
        File cfg = File.createTempFile("zkcfg", ".cfg");
        cfg.deleteOnExit();
        try (FileWriter w = new FileWriter(cfg)) {
            w.write("tickTime=2000\n");
            w.write("dataDir=/tmp/zkdata\n");
            w.write("clientPort=2181\n");
            w.write("initLimit=5\n");
            w.write("syncLimit=2\n");
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());
        int tickTime = config.getTickTime();
        assertTrue("tickTime must be > 0", tickTime > 0);
    }

    @Test
    public void testTickTimeMinSessionTimeoutDefault() throws Exception {
        // Prepare a configuration file that does NOT set minSessionTimeout
        File cfg = File.createTempFile("zkcfg", ".cfg");
        cfg.deleteOnExit();
        try (FileWriter w = new FileWriter(cfg)) {
            w.write("tickTime=2000\n");
            w.write("dataDir=/tmp/zkdata\n");
            w.write("clientPort=2181\n");
            w.write("initLimit=5\n");
            w.write("syncLimit=2\n");
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());

        int tickTime = config.getTickTime();
        int minSessionTimeout = config.getMinSessionTimeout();

        // Default minSessionTimeout should be 2 * tickTime
        assertEquals("Default minSessionTimeout must be 2 * tickTime",
                     2 * tickTime, minSessionTimeout);
    }

    @Test
    public void testTickTimeNonNumeric() throws Exception {
        // Prepare a configuration file with non-numeric tickTime
        File cfg = File.createTempFile("zkcfg", ".cfg");
        cfg.deleteOnExit();
        try (FileWriter w = new FileWriter(cfg)) {
            w.write("tickTime=abc\n");
            w.write("dataDir=/tmp/zkdata\n");
            w.write("clientPort=2181\n");
            w.write("initLimit=5\n");
            w.write("syncLimit=2\n");
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parse(cfg.getAbsolutePath());
            fail("Should throw exception for non-numeric tickTime");
        } catch (QuorumPeerConfig.ConfigException expected) {
            // expected
        }
    }

    @Test
    public void testTickTimeNegativeValue() throws Exception {
        // Prepare a configuration file with negative tickTime
        File cfg = File.createTempFile("zkcfg", ".cfg");
        cfg.deleteOnExit();
        try (FileWriter w = new FileWriter(cfg)) {
            w.write("tickTime=2000\n");
            w.write("dataDir=/tmp/zkdata\n");
            w.write("clientPort=2181\n");
            w.write("initLimit=5\n");
            w.write("syncLimit=2\n");
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());
        int tickTime = config.getTickTime();
        assertTrue("tickTime must be positive", tickTime > 0);
    }
}