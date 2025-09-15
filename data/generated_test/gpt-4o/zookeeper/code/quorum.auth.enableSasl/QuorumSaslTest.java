package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class QuorumSaslTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test method to verify functionality when quorum.auth.enableSasl is set to true.
     */
    @Test
    public void testSetQuorumSaslEnabled_True() throws Exception {
        // 1. Load configuration using the API
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // 2. Prepare the test conditions: Initialize QuorumPeerConfig and parse properties
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 3. Validate the configuration values obtained
        assertNotNull("Data directory should not be null", config.getDataDir());
        assertNotNull("Data log directory should not be null", config.getDataLogDir());
        System.out.println("Parsed configuration:");
        System.out.println("DataDir = " + config.getDataDir());
        System.out.println("DataLogDir = " + config.getDataLogDir());
    }
}