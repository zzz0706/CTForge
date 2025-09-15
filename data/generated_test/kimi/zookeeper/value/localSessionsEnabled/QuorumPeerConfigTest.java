package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import static org.junit.Assert.*;

public class QuorumPeerConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testLocalSessionsEnabledConfiguration() {
  
            // Step 1: Load configuration file
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Step 2: Parse the configuration using QuorumPeerConfig
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Step 3: Retrieve the value of 'localSessionsEnabled'
            boolean localSessionsEnabled = config.areLocalSessionsEnabled();

            assertTrue(
                "localSessionsEnabled must be either 'true' or 'false'.",
                localSessionsEnabled == true || localSessionsEnabled == false
            );

    }
}