package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Properties;

public class QuorumCnxnThreadsSizeConfigTest {

    private QuorumPeerConfig config;

    @Before
    public void setUp() {
        config = new QuorumPeerConfig();
    }

    @After
    public void tearDown() {
        config = null;
    }

    @Test
    public void testQuorumCnxnThreadsSizeValid() throws Exception {
        // Prepare test conditions: load a valid configuration
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("server.1", "localhost:2888:3888");
        props.setProperty("quorum.cnxn.threads.size", "20");
        config.parseProperties(props);

        // Test code: verify the parsed value
        int actual = config.quorumCnxnThreadsSize;
        assertEquals("quorum.cnxn.threads.size must be exactly 20", 20, actual);
    }

    @Test
    public void testQuorumCnxnThreadsSizeBelowDefault() throws Exception {
        // Prepare test conditions: load configuration with value below default
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("server.1", "localhost:2888:3888");
        props.setProperty("quorum.cnxn.threads.size", "2");
        config.parseProperties(props);

        // Test code: verify the value is accepted as-is (no fallback)
        int actual = config.quorumCnxnThreadsSize;
        assertEquals("quorum.cnxn.threads.size below default is accepted", 2, actual);
    }

    @Test
    public void testQuorumCnxnThreadsSizeZero() throws Exception {
        // Prepare test conditions: load configuration with zero value
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("server.1", "localhost:2888:3888");
        props.setProperty("quorum.cnxn.threads.size", "0");
        config.parseProperties(props);

        // Test code: verify the value is accepted as-is
        int actual = config.quorumCnxnThreadsSize;
        assertEquals("quorum.cnxn.threads.size zero is accepted", 0, actual);
    }

    @Test
    public void testQuorumCnxnThreadsSizeNegative() throws Exception {
        // Prepare test conditions: load configuration with negative value
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("server.1", "localhost:2888:3888");
        props.setProperty("quorum.cnxn.threads.size", "-5");
        config.parseProperties(props);

        // Test code: verify the value is accepted as-is
        int actual = config.quorumCnxnThreadsSize;
        assertEquals("quorum.cnxn.threads.size negative is accepted", -5, actual);
    }

    @Test
    public void testQuorumCnxnThreadsSizeNotSet() throws Exception {
        // Prepare test conditions: load configuration without the property
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("server.1", "localhost:2888:3888");
        config.parseProperties(props);

        // Test code: verify the default value is used
        int actual = config.quorumCnxnThreadsSize;
        assertEquals("quorum.cnxn.threads.size not set should use default", 0, actual);
    }
}