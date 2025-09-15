package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class QuorumPeerConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";
    private QuorumPeerConfig quorumPeerConfig;

    @Before
    public void setUp() throws Exception {
        // Prepare the test conditions: Load configuration properties from the external config file.
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Initialize QuorumPeerConfig with the loaded properties.
        quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(props);
    }

    @Test
    public void testConfigValuesAreLoadedCorrectly() throws Exception {
        // Use the zookeeper3.5.6 API correctly to obtain configuration values, ensuring we do not hardcode the configuration values.

        // Extract expected values dynamically from the configuration file for verification.
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        int expectedTickTime = Integer.parseInt(props.getProperty("tickTime", "3000")); // tickTime from ctest.cfg
        int expectedInitLimit = Integer.parseInt(props.getProperty("initLimit", "10")); // initLimit from ctest.cfg
        int expectedSyncLimit = Integer.parseInt(props.getProperty("syncLimit", "5")); // syncLimit from ctest.cfg
        String expectedDataDir = new File(props.getProperty("dataDir", "/home/zzz/zookeeper/data")).getAbsolutePath();
        String expectedDataLogDir = new File(props.getProperty("dataLogDir", "/home/zzz/zookeeper/log")).getAbsolutePath();
        int expectedClientPort = Integer.parseInt(props.getProperty("clientPort", "2181")); // clientPort from ctest.cfg

        // Verify loaded config values from the QuorumPeerConfig object.
        assertEquals("Expect tickTime to be correct", expectedTickTime, quorumPeerConfig.getTickTime());
        assertEquals("Expect initLimit to be correct", expectedInitLimit, quorumPeerConfig.getInitLimit());
        assertEquals("Expect syncLimit to be correct", expectedSyncLimit, quorumPeerConfig.getSyncLimit());
        assertEquals("Expect dataDir to be correct", expectedDataDir, quorumPeerConfig.getDataDir().getAbsolutePath());
        assertEquals("Expect dataLogDir to be correct", expectedDataLogDir, quorumPeerConfig.getDataLogDir().getAbsolutePath());

        // Verify client port value.
        InetSocketAddress clientPortAddr = quorumPeerConfig.getClientPortAddress();
        assertEquals("Expect client port to be correctly parsed from the config file", expectedClientPort, clientPortAddr.getPort());
    }
}