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
        int maxSessionTimeout = config.getMaxSessionTimeout(); // maxSessionTimeout configuration

        // Step 4: Validate the constraints and dependencies.
        // Constraint: maxSessionTimeout should default to 20 times the tickTime if not explicitly set.
        int expectedDefaultMaxSessionTimeout = tickTime * 20;

        // Verify the constraints
        Assert.assertTrue(
                "maxSessionTimeout should not be less than 20 times tickTime.",
                maxSessionTimeout >= expectedDefaultMaxSessionTimeout
        );

        // Additional sanity check to ensure tickTime is positive
        Assert.assertTrue(
                "tickTime configuration value must be positive.",
                tickTime > 0
        );

        // Log the output to give a better understanding of what was validated.
        System.out.println("Validation Successful:");
        System.out.println("tickTime = " + tickTime);
        System.out.println("maxSessionTimeout = " + maxSessionTimeout);
    }
}