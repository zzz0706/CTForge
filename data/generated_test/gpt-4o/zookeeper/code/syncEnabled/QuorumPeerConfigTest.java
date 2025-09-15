package org.apache.zookeeper.server.quorum;

import org.junit.Assert;
import org.junit.Test;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class QuorumPeerConfigTest {

    // Load the configuration
    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testQuorumPeerGetSyncEnabledWithConfigValue() {
        try {
            // Step 1: Parse configuration from the file using ZooKeeper API
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
            quorumPeerConfig.parseProperties(props);

            // Step 2: Verify the configuration is properly loaded
            boolean expectedSyncEnabled = quorumPeerConfig.getSyncEnabled();

            // Step 3: Prepare QuorumPeer instance with the loaded configuration
            QuorumPeer quorumPeer = new QuorumPeer();
            quorumPeer.setSyncEnabled(quorumPeerConfig.getSyncEnabled());

            // Step 4: Verify the value matches the configuration via QuorumPeer API
            Assert.assertEquals("syncEnabled value should match the configuration file value", 
                                expectedSyncEnabled, quorumPeer.getSyncEnabled());
        } catch (Exception e) {
            // Handle exception
            e.printStackTrace();
            Assert.fail("Exception occurred during test execution: " + e.getMessage());
        }
    }
}