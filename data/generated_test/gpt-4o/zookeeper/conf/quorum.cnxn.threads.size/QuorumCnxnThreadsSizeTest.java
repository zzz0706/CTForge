package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test to validate the "quorum.cnxn.threads.size" configuration in Zookeeper 3.5.6.
 */
public class QuorumCnxnThreadsSizeTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * This test validates the configuration "quorum.cnxn.threads.size" and ensures that it satisfies its constraints.
     */
    @Test
    public void testQuorumCnxnThreadsSizeConfiguration() {
        try {
            // Step 1: Load configuration properties from the given file path
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Step 2: Parse properties into QuorumPeerConfig object
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Step 3: Retrieve the "quorum.cnxn.threads.size" configuration
            // Use the properties directly to access the configuration as QuorumPeerConfig does not provide a direct getter for this property.
            String quorumCnxnThreadsSizeStr = props.getProperty("quorum.cnxn.threads.size");

            if (quorumCnxnThreadsSizeStr == null || quorumCnxnThreadsSizeStr.isEmpty()) {
                fail("Configuration 'quorum.cnxn.threads.size' not found in properties.");
            }

            int quorumCnxnThreadsSize;
            try {
                quorumCnxnThreadsSize = Integer.parseInt(quorumCnxnThreadsSizeStr);
            } catch (NumberFormatException e) {
                fail("Configuration value for 'quorum.cnxn.threads.size' is not a valid integer: " + quorumCnxnThreadsSizeStr);
                return; // This return is just to stop execution in case of a fail.
            }

            // Step 4: Validate the configuration constraints
            // Constraint: The size should be a positive integer greater than zero.
            assertTrue("Configuration value for 'quorum.cnxn.threads.size' must be a positive integer greater than 0.",
                    quorumCnxnThreadsSize > 0);

        } catch (Exception e) {
            // Fail the test if any unexpected exception occurs
            fail("Unexpected error during test execution: " + e.getMessage());
        }
    }
}