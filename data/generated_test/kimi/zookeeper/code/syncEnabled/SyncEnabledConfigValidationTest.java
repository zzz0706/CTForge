package org.apache.zookeeper.server.quorum;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

import static org.junit.Assert.*;

public class SyncEnabledConfigValidationTest {

    private QuorumPeerConfig config;

    @Before
    public void setUp() {
        config = new QuorumPeerConfig();
    }

    @After
    public void tearDown() {
        System.clearProperty("zookeeper.observer.syncEnabled");
    }

    @Test
    public void testSyncEnabledValidTrue() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", System.getProperty("java.io.tmpdir") + File.separator + "zk_test_" + System.nanoTime());
        props.setProperty("syncEnabled", "true");
        config.parseProperties(props);
        assertTrue("syncEnabled should be true when set to 'true'", config.getSyncEnabled());
    }

    @Test
    public void testSyncEnabledValidFalse() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", System.getProperty("java.io.tmpdir") + File.separator + "zk_test_" + System.nanoTime());
        props.setProperty("syncEnabled", "false");
        config.parseProperties(props);
        assertFalse("syncEnabled should be false when set to 'false'", config.getSyncEnabled());
    }

    @Test
    public void testSyncEnabledInvalidValue() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", System.getProperty("java.io.tmpdir") + File.separator + "zk_test_" + System.nanoTime());
        props.setProperty("syncEnabled", "invalid");
        config.parseProperties(props);
        // ZooKeeper 3.5.6 silently defaults to false for invalid boolean values
        assertFalse("syncEnabled defaults to false for invalid value", config.getSyncEnabled());
    }

    @Test
    public void testSyncEnabledSystemPropertyOverride() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", System.getProperty("java.io.tmpdir") + File.separator + "zk_test_" + System.nanoTime());
        props.setProperty("syncEnabled", "false");
        config.parseProperties(props);
        assertFalse("syncEnabled should be false before system property override", config.getSyncEnabled());
        
        // Set system property to override
        System.setProperty("zookeeper.observer.syncEnabled", "true");
        // Create a new config instance to pick up the system property
        QuorumPeerConfig newConfig = new QuorumPeerConfig();
        newConfig.parseProperties(props);
        assertFalse("System property zookeeper.observer.syncEnabled does NOT override file config", newConfig.getSyncEnabled());
    }

    @Test
    public void testSyncEnabledDefaultValue() throws Exception {
        Properties props = new Properties();
        props.setProperty("dataDir", System.getProperty("java.io.tmpdir") + File.separator + "zk_test_" + System.nanoTime());
        config.parseProperties(props);
        assertTrue("syncEnabled should default to true when not specified", config.getSyncEnabled());
    }
}