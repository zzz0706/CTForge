package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class QuorumAuthConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testQuorumAuthServerSaslLoginContext() {
        // Step 1: Load the configuration file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse properties into the ZooKeeper configuration object
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 3: Retrieve the configuration value
        String quorumServerLoginContext = props.getProperty("quorum.auth.server.saslLoginContext");

        Assert.assertTrue("Ensure correct configuration propagation for quorumServerLoginContext.",
                quorumServerLoginContext.matches("^[a-zA-Z0-9._-]+$")); 

    }
}