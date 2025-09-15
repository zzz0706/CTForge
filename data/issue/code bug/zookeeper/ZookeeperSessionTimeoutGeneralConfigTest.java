package org.apache.zookeeper.server.quorum;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

//ZOOKEEPER-1213
public class ZookeeperSessionTimeoutGeneralConfigTest {

    // Path to your external configuration file
    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testSessionTimeoutConstraints() {
        // Load properties from the configuration file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        } catch (Exception e) {
            fail("Failed to load config file: " + e.getMessage());
        }

        // Parse properties using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (Exception e) {
            fail("Failed to parse config: " + e.getMessage());
        }

        int tickTime = config.getTickTime();
        int minSessionTimeout = config.getMinSessionTimeout();
        int maxSessionTimeout = config.getMaxSessionTimeout();

        assertTrue("tickTime should be positive", tickTime > 0);
        assertTrue("minSessionTimeout should be >= tickTime", minSessionTimeout >= tickTime);
        assertTrue("maxSessionTimeout should be >= tickTime", maxSessionTimeout >= tickTime);
        assertTrue("minSessionTimeout should not exceed maxSessionTimeout",
                minSessionTimeout <= maxSessionTimeout);

        // Optionally, log for manual review
        System.out.println(String.format("tickTime=%d, minSessionTimeout=%d, maxSessionTimeout=%d",
                tickTime, minSessionTimeout, maxSessionTimeout));
    }
}
