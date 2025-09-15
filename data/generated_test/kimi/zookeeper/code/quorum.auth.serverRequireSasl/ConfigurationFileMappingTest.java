package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Verifies that configuration parameters from a properties file are correctly
 * mapped to the fields of a QuorumPeerConfig object after parsing.
 */
public class ConfigurationFileMappingTest {

    private static final String TEST_CONFIG_PATH = "ctest.cfg";
    
    private Properties sourceProperties;
    private QuorumPeerConfig parsedConfig;

    /**
     * Prepares for each test by loading a properties file and using it to
     * parse a new QuorumPeerConfig instance.
     */
    @Before
    public void initializeConfiguration() throws Exception {
        // Load the source properties directly from the configuration file.
        sourceProperties = new Properties();
        try (InputStream stream = new FileInputStream(TEST_CONFIG_PATH)) {
            sourceProperties.load(stream);
        }

        // Use the loaded properties to create the configuration object to be tested.
        parsedConfig = new QuorumPeerConfig();
        parsedConfig.parseProperties(sourceProperties);
    }

    /**
     * This test compares the raw values from the properties file against the
     * values retrieved from the parsed QuorumPeerConfig object to ensure
     * they match exactly.
     */
    @Test
    public void testSourcePropertiesMatchParsedConfigValues() {
        // --- 1. Extract Expected Values from Source Properties ---
        int expectedTickTime = Integer.parseInt(sourceProperties.getProperty("tickTime"));
        int expectedInitLimit = Integer.parseInt(sourceProperties.getProperty("initLimit"));
        int expectedSyncLimit = Integer.parseInt(sourceProperties.getProperty("syncLimit"));
        String expectedDataDirPath = new File(sourceProperties.getProperty("dataDir")).getAbsolutePath();
        String expectedLogDirPath = new File(sourceProperties.getProperty("dataLogDir")).getAbsolutePath();
        int expectedClientPort = Integer.parseInt(sourceProperties.getProperty("clientPort"));

        // --- 2. Verify Parsed Values Against Expected Values ---

        // Verify time-based parameters
        assertEquals("tickTime from file should match parsed value", expectedTickTime, parsedConfig.getTickTime());
        assertEquals("initLimit from file should match parsed value", expectedInitLimit, parsedConfig.getInitLimit());
        assertEquals("syncLimit from file should match parsed value", expectedSyncLimit, parsedConfig.getSyncLimit());

        // Verify directory path parameters
        assertEquals("Data directory path should match parsed value", expectedDataDirPath, parsedConfig.getDataDir().getAbsolutePath());
        assertEquals("Log directory path should match parsed value", expectedLogDirPath, parsedConfig.getDataLogDir().getAbsolutePath());

        // Verify network parameters
        InetSocketAddress clientAddress = parsedConfig.getClientPortAddress();
        assertEquals("Client port should match parsed value", expectedClientPort, clientAddress.getPort());
    }
}