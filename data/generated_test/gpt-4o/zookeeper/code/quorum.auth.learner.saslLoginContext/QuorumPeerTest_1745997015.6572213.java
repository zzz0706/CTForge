package org.apache.zookeeper.server.quorum.auth;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class QuorumPeerTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Helper method to load properties from the configuration file.
     */
    private Properties loadProperties(String configPath) throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(configPath)) {
            properties.load(in);
        }
        return properties;
    }

    @Test
    public void testInitialize_withValidConfiguration() throws Exception {
        // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Properties props = loadProperties(CONFIG_PATH);

        QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
        quorumConfig.parseProperties(props);

        // 2. Prepare the test conditions.
        System.out.println("tickTime       = " + quorumConfig.getTickTime());
        System.out.println("initLimit      = " + quorumConfig.getInitLimit());
        System.out.println("syncLimit      = " + quorumConfig.getSyncLimit());
        System.out.println("dataDir        = " + quorumConfig.getDataDir());
        System.out.println("dataLogDir     = " + quorumConfig.getDataLogDir());

        // 3. Test code - Verify the configuration values are correctly loaded.
        assert quorumConfig.getDataDir() != null : "Data directory is null.";
        assert quorumConfig.getDataLogDir() != null : "Data log directory is null.";
        assert quorumConfig.getTickTime() > 0 : "Invalid tickTime value.";
        assert quorumConfig.getInitLimit() > 0 : "Invalid initLimit value.";
        assert quorumConfig.getSyncLimit() > 0 : "Invalid syncLimit value.";

        // 4. Code after testing - Output values to confirm correct initialization
        System.out.println("Configuration values successfully initialized.");
    }
}