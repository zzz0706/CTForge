package org.apache.zookeeper.server.quorum;

import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Validates the parsing and retrieval of critical directory settings from the ZooKeeper configuration.
 */
public class DirectorySettingsValidationTest {

    private static final String TEST_CONFIG_PATH = "ctest.cfg";

    /**
     * This test ensures that the paths for the data directory (for snapshots) and the
     * data log directory (for transaction logs) are correctly parsed from the configuration file.
     */
    @Test
    public void testSnapshotAndLogDirectoriesAreCorrectlyConfigured() throws Exception {
        // Given a valid configuration file, when it is loaded and parsed.
        QuorumPeerConfig peerConfig = loadAndParseConfiguration(TEST_CONFIG_PATH);

        // When the directory settings are retrieved from the config object.
        File snapshotDir = peerConfig.getDataDir();
        File logDir = peerConfig.getDataLogDir();

        // Then, the retrieved directory paths should be valid and correctly configured.
        Assert.assertNotNull(
            "Snapshot directory (dataDir) should not be null after parsing.",
            snapshotDir
        );
        Assert.assertNotNull(
            "Transaction log directory (dataLogDir) should not be null after parsing.",
            logDir
        );

        // Add a sanity check to ensure the paths are absolute, as is typical.
        Assert.assertTrue(
            "The configured snapshot directory path should be absolute.",
            snapshotDir.isAbsolute()
        );
        Assert.assertTrue(
            "The configured log directory path should be absolute.",
            logDir.isAbsolute()
        );
    }

    /**
     * Helper method to load a configuration file and parse it into a QuorumPeerConfig object.
     *
     * @param path The path to the configuration file.
     * @return A fully parsed {@link QuorumPeerConfig} instance.
     * @throws IOException If the file cannot be read.
     * @throws QuorumPeerConfig.ConfigException If the configuration is invalid.
     */
    private QuorumPeerConfig loadAndParseConfiguration(String path)
            throws IOException, QuorumPeerConfig.ConfigException {

        Properties loadedProperties = new Properties();
        try (InputStream stream = new FileInputStream(path)) {
            loadedProperties.load(stream);
        }

        QuorumPeerConfig parsedConfig = new QuorumPeerConfig();
        parsedConfig.parseProperties(loadedProperties);
        return parsedConfig;
    }
}