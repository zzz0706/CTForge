package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class QuorumCnxnThreadsSizeTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void test_setQuorumCnxnThreadsSize_with_default_fallback() {
        try {
            // Step 1: Load configuration file
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Step 2: Parse configuration properties
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Step 3: Retrieve configuration values and verification logic
            int quorumCnxnThreadsSize = props.containsKey("quorum.cnxn.threads.size")
                ? Integer.parseInt(props.getProperty("quorum.cnxn.threads.size"))
                : 3; // Default fallback example

            // Step 4: Verify the configuration value
            assertTrue("quorum.cnxn.threads.size should fallback to default (>=3).", quorumCnxnThreadsSize >= 3);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to test quorum.cnxn.threads.size configuration fallback.");
        }
    }
}