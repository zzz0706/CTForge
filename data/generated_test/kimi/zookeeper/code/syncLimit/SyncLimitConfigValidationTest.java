package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.*;

import java.io.File;
import java.io.StringReader;
import java.util.Properties;

import org.junit.Test;

public class SyncLimitConfigValidationTest {

    private Properties minimalProps() {
        Properties p = new Properties();
        p.setProperty("dataDir", System.getProperty("java.io.tmpdir") + File.separator + "zk_test");
        p.setProperty("clientPort", "2181");
        p.setProperty("server.1", "127.0.0.1:2888:3888");
        // syncLimit has a default value of 5 in 3.5.6, so it does not throw
        // IllegalArgumentException when missing.  To trigger the exception we
        // must explicitly set it to an invalid value.
        return p;
    }

    @Test
    public void testSyncLimitValidPositiveInteger() throws Exception {
        Properties props = minimalProps();
        props.load(new StringReader("syncLimit=5"));
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);
        assertEquals("syncLimit should be parsed as 5", 5, config.syncLimit);
    }

    @Test
    public void testSyncLimitZero() throws Exception {
        Properties props = minimalProps();
        props.load(new StringReader("syncLimit=0"));
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);
        assertEquals("syncLimit=0 should be accepted", 0, config.syncLimit);
    }

    @Test
    public void testSyncLimitNegative() throws Exception {
        Properties props = minimalProps();
        props.load(new StringReader("syncLimit=-3"));
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);
        assertEquals("syncLimit=-3 should be accepted", -3, config.syncLimit);
    }

    @Test
    public void testSyncLimitNonInteger() throws Exception {
        Properties props = minimalProps();
        props.load(new StringReader("syncLimit=abc"));
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
            fail("syncLimit=abc should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testSyncLimitMissing() throws Exception {
        Properties props = minimalProps();
        props.load(new StringReader(""));
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);
        assertEquals("syncLimit should default to 0", 0, config.syncLimit);
    }
}