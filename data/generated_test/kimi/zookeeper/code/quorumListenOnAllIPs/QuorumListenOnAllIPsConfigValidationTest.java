package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Properties;

public class QuorumListenOnAllIPsConfigValidationTest {

    private QuorumPeerConfig config;

    @Before
    public void setUp() throws Exception {
        config = new QuorumPeerConfig();
    }

    @After
    public void tearDown() throws Exception {
        config = null;
    }

    @Test
    public void testQuorumListenOnAllIPsValidTrue() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("quorumListenOnAllIPs", "true");
        config.parseProperties(props);

        boolean value = config.getQuorumListenOnAllIPs();
        assertTrue("quorumListenOnAllIPs should be true", value);
    }

    @Test
    public void testQuorumListenOnAllIPsValidFalse() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("quorumListenOnAllIPs", "false");
        config.parseProperties(props);

        boolean value = config.getQuorumListenOnAllIPs();
        assertFalse("quorumListenOnAllIPs should be false", value);
    }

    @Test
    public void testQuorumListenOnAllIPsInvalidString() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("quorumListenOnAllIPs", "invalid");
        config.parseProperties(props);
        // ZooKeeper silently ignores invalid boolean strings and defaults to false
        assertFalse("quorumListenOnAllIPs should default to false for invalid string", config.getQuorumListenOnAllIPs());
    }

    @Test
    public void testQuorumListenOnAllIPsEmpty() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("quorumListenOnAllIPs", "");
        config.parseProperties(props);
        // ZooKeeper silently ignores empty values and defaults to false
        assertFalse("quorumListenOnAllIPs should default to false for empty string", config.getQuorumListenOnAllIPs());
    }

    @Test
    public void testQuorumListenOnAllIPsNotSet() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        config.parseProperties(props);

        boolean value = config.getQuorumListenOnAllIPs();
        assertFalse("quorumListenOnAllIPs should be false when not set", value);
    }
}