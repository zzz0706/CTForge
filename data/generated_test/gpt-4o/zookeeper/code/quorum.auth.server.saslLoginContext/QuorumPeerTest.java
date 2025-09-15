package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class QuorumPeerTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    // Test case for setting the quorum server directories
    // 1. Use the zookeeper3.5.6 API to parse configuration values.
    // 2. Set up mocked objects and prepare the test conditions.
    // 3. Verify the behavior of QuorumPeerConfig in storing and retrieving configuration values for dataDir and dataLogDir.
    public void testSetQuorumServerDirectories() {
        try {
            // 1. Load configuration properties using the ZooKeeper config API.
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Parse configuration
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);

            // Mock the QuorumPeerConfig object
            QuorumPeerConfig configMock = Mockito.mock(QuorumPeerConfig.class);
            Mockito.when(configMock.getDataDir()).thenReturn(config.getDataDir());
            Mockito.when(configMock.getDataLogDir()).thenReturn(config.getDataLogDir());

            // Fetch mocked configuration values
            File dataDir = configMock.getDataDir();
            File dataLogDir = configMock.getDataLogDir();

            // 3. Verify that the mock returns the expected values
            Assert.assertNotNull(configMock.getDataDir());
            Assert.assertEquals(dataDir, configMock.getDataDir());

            Assert.assertNotNull(configMock.getDataLogDir());
            Assert.assertEquals(dataLogDir, configMock.getDataLogDir());

            // 4. Assert that directories match real configuration values
            Assert.assertEquals(config.getDataDir(), dataDir);
            Assert.assertEquals(config.getDataLogDir(), dataLogDir);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Test case failed due to exception: " + e.getMessage());
        }
    }
}