package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Unit test for verifying the validity of the configuration "autopurge.purgeInterval" in Zookeeper 3.5.6.
 */
public class QuorumPeerConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to validate the configuration "autopurge.purgeInterval".
     * 
     * Steps:
     * 1. Read the configuration value from the file using the Zookeeper API.
     * 2. Validate that the configuration value satisfies its constraints:
     *    - Should be a non-negative to enable auto purging.
     *    
     */
    @Test
    public void testPurgeIntervalConfiguration() throws Exception {
        // Step 1: Load properties from the configuration file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse properties into QuorumPeerConfig object
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 3: Retrieve the "autopurge.purgeInterval" value
        int purgeInterval = config.getPurgeInterval();

        // Step 4: Validate the configuration value based on its constraints
        assertTrue("autopurge.purgeInterval should be a non-negative.", purgeInterval >= 0);
        
    }
}