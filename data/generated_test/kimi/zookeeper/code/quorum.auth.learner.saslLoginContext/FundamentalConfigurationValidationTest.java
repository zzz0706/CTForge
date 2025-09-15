package org.apache.zookeeper.server.quorum.auth;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Validates that fundamental parameters from a ZooKeeper configuration file are loaded and parsed correctly.
 */
public class FundamentalConfigurationValidationTest {

    private static final String TEST_CONFIG_PATH = "ctest.cfg";

    /**
     * This test ensures that after parsing a valid configuration file, all core server
     * properties (like tickTime, syncLimit, and data directories) are initialized with valid, non-default values.
     */
    @Test
    public void testCoreServerPropertiesAreLoadedCorrectly() throws Exception {
        // Given a valid configuration file, when it is parsed.
        QuorumPeerConfig serverConfig = loadAndParseConfig(TEST_CONFIG_PATH);

        // Then, all fundamental configuration parameters should be valid.
        assertNotNull("The data directory must be configured.", serverConfig.getDataDir());
        assertNotNull("The transaction log directory must be configured.", serverConfig.getDataLogDir());

        assertTrue("The tickTime must be a positive value.", serverConfig.getTickTime() > 0);
        assertTrue("The initLimit must be a positive value.", serverConfig.getInitLimit() > 0);
        assertTrue("The syncLimit must be a positive value.", serverConfig.getSyncLimit() > 0);
    }

    /**
     * A helper method that loads a properties file from the given path and
     * uses it to initialize a QuorumPeerConfig instance.
     *
     * @param configPath The path to the configuration file.
     * @return A fully parsed QuorumPeerConfig object.
     * @throws IOException If the file cannot be read.
     * @throws QuorumPeerConfig.ConfigException If the configuration properties are invalid.
     */
    private QuorumPeerConfig loadAndParseConfig(String configPath) throws IOException, QuorumPeerConfig.ConfigException {
        Properties loadedProperties = new Properties();
        try (InputStream stream = new FileInputStream(configPath)) {
            loadedProperties.load(stream);
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(loadedProperties);
        return config;
    }
}