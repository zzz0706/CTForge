package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;
//ZOOKEEPER-2579 
public class DataAndSnapDirWritableConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test whether dataDir and snapDir configured in config file are writable.
     * Only checks existence and writability, does NOT change real server state.
     */
    @Test
    public void testDataDirAndSnapDirWritableFromConfig() throws Exception {
        // Load properties from config file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Parse properties using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (Exception e) {
            fail("Failed to parse config: " + e.getMessage());
        }

        // Get dataDir and snapDir from config (snapDir is optional, may not be set)
        File dataDir = config.getDataDir();
        File snapDir = config.getDataLogDir();

        // Check dataDir
        assertNotNull("dataDir should not be null", dataDir);
        assertTrue("dataDir does not exist: " + dataDir.getAbsolutePath(), dataDir.exists());
        assertTrue("dataDir is not a directory: " + dataDir.getAbsolutePath(), dataDir.isDirectory());
        assertTrue("dataDir is not writable: " + dataDir.getAbsolutePath(), dataDir.canWrite());

        // Check snapDir (if specified)
        if (snapDir != null && !snapDir.equals(dataDir)) {
            assertTrue("snapDir does not exist: " + snapDir.getAbsolutePath(), snapDir.exists());
            assertTrue("snapDir is not a directory: " + snapDir.getAbsolutePath(), snapDir.isDirectory());
            assertTrue("snapDir is not writable: " + snapDir.getAbsolutePath(), snapDir.canWrite());
        }
    }
}
