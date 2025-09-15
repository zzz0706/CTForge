package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

/**
 * Validates that essential configuration properties are correctly loaded and parsed.
 */
public class CoreConfigurationLoadingTest {

    private static final String TEST_CONFIG_FILE_PATH = "ctest.cfg";

    /**
     * This test ensures that fundamental directory paths, such as the data and transaction log directories,
     * are successfully parsed from the configuration file.
     */
    @Test
    public void testEssentialDirectoriesAreConfigured() throws Exception {
        // Given a path to a valid configuration file.
        // When the configuration is loaded and parsed.
        QuorumPeerConfig peerConfig = loadConfigurationForTest(TEST_CONFIG_FILE_PATH);

        // Then the critical directory configurations should be available.
        assertNotNull(
            "The data directory (dataDir) is a critical setting and must be present.",
            peerConfig.getDataDir()
        );

        assertNotNull(
            "The data log directory (dataLogDir) is essential for transactions and must be present.",
            peerConfig.getDataLogDir()
        );
    }

    /**
     * A helper method to load properties from a file and parse them into a QuorumPeerConfig object.
     *
     * @param configPath The file system path to the configuration file.
     * @return A parsed QuorumPeerConfig instance.
     * @throws IOException If the file cannot be read.
     * @throws QuorumPeerConfig.ConfigException If the properties are invalid.
     */
    private QuorumPeerConfig loadConfigurationForTest(String configPath)
            throws IOException, QuorumPeerConfig.ConfigException {
        
        Properties configurationProperties = new Properties();
        try (InputStream stream = new FileInputStream(configPath)) {
            configurationProperties.load(stream);
        }

        QuorumPeerConfig parsedConfig = new QuorumPeerConfig();
        parsedConfig.parseProperties(configurationProperties);
        
        return parsedConfig;
    }
}