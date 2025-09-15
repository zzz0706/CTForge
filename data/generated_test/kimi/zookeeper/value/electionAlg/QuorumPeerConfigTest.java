package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class QuorumPeerConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    private Properties loadConfiguration() throws IOException {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }
        return props;
    }

    @Test
    public void testElectionAlgValidity() throws Exception {
        // 1. Load configuration from file.
        Properties props = loadConfiguration();

        // 2. Parse the configuration using QuorumPeerConfig.
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 3. Test the election algorithm configuration.
        int electionAlg = config.getElectionAlg();
        assertTrue(
            "electionAlg must be one of {0, 1, 2, 3}",
            electionAlg == 0 || electionAlg == 1 || electionAlg == 2 || electionAlg == 3
        );
    }
}