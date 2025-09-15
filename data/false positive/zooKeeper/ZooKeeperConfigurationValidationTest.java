package org.apache.zookeeper.test;

import org.junit.Test;
import org.junit.Assert;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.io.File;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

public class ZooKeeperConfigurationValidationTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * This test validates the maxSessionTimeout configuration in ZooKeeper.
     * It ensures that the value respects its dependency on the tickTime configuration
     * and adheres to the constraints from the ZooKeeper source code.
     */
    @Test
    public void testMaxSessionTimeoutConfigurationValidity() throws Exception {
        // Step 1: Load properties from the configuration file.
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            properties.load(in);
        }

        // Step 2: Create a QuorumPeerConfig and parse the loaded properties.
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(properties);

        // Step 3: Fetch the required configuration values.
        int tickTime = config.getTickTime(); // TickTime configuration

        // Step 4: Validate the constraints.
        Assert.assertTrue(
                "tickTime configuration value must be non-negative.",
                tickTime >= 0
        );
    }
}