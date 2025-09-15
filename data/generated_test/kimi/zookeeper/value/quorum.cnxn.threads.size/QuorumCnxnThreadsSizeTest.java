package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test to validate the "quorum.cnxn.threads.size" configuration in
 * Zookeeper 3.5.6.
 */
public class QuorumCnxnThreadsSizeTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * This test validates the configuration "quorum.cnxn.threads.size" and ensures
     * that it satisfies its constraints.
     */
    @Test
    public void testQuorumCnxnThreadsSizeConfiguration() {


            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);      
            String quorumCnxnThreadsSizeStr = props.getProperty("quorum.cnxn.threads.size");
       
            int quorumCnxnThreadsSize = Integer.parseInt(quorumCnxnThreadsSizeStr);
    
            assertTrue("Configuration value for 'quorum.cnxn.threads.size' must be a positive inte
                        er greater than 0.",
                    quorumCnxnThreadsSize > 0);
    }
}