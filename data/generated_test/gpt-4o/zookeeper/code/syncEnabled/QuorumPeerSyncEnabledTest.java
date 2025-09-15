package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.ObserverZooKeeperServer;
import org.apache.zookeeper.server.Request;
import org.junit.Test;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class QuorumPeerSyncEnabledTest {

    // Path to ZooKeeper configuration file
    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testQuorumPeerGetSyncEnabledWithSystemPropertyOverride() throws Exception {
        // Load the ZooKeeper configuration from the specified file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Mock the QuorumPeerConfig to retrieve the configuration value
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.getSyncEnabled()).thenReturn(Boolean.parseBoolean(props.getProperty("syncEnabled", "true")));

        // Prepare the system property override
        System.setProperty("zookeeper.observer.syncEnabled", "false");

        // Step 3: Initialize a QuorumPeer and set the syncEnabled value
        QuorumPeer quorumPeer = new QuorumPeer();
        quorumPeer.setSyncEnabled(configMock.getSyncEnabled());

        // Step 4: Verify the behavior of getSyncEnabled
        boolean result = quorumPeer.getSyncEnabled();

        // Step 5: Validate that the system property takes precedence and returns false
        assertFalse("System property override should take precedence, resulting in false.", result);

        // Clean up the system property after the test
        System.clearProperty("zookeeper.observer.syncEnabled");
    }
}