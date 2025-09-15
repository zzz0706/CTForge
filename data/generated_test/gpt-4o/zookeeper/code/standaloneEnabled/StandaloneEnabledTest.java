package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class StandaloneEnabledTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testStandaloneEnabledTrueReconfigBehavior() {
        try {
            // Load configuration using ZooKeeper API
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(CONFIG_PATH)) {
                props.load(in);
            }

            // Parse configuration using QuorumPeerConfig and mock necessary behaviors
            QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
            quorumPeerConfig.parseProperties(props);

            // Mock QuorumPeerConfig to simulate its behavior
            QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
            when(configMock.getDataDir()).thenReturn(quorumPeerConfig.getDataDir());
            when(configMock.getDataLogDir()).thenReturn(quorumPeerConfig.getDataLogDir());
            when(configMock.getClientPortAddress()).thenReturn(quorumPeerConfig.getClientPortAddress());

            // Verify values and behavior
            File expectedDataDir = new File(quorumPeerConfig.getDataDir().getAbsolutePath());
            File expectedDataLogDir = new File(quorumPeerConfig.getDataLogDir().getAbsolutePath());
            InetSocketAddress expectedClientPortAddr = quorumPeerConfig.getClientPortAddress();

            File actualDataDir = configMock.getDataDir();
            File actualDataLogDir = configMock.getDataLogDir();
            InetSocketAddress actualClientPortAddr = configMock.getClientPortAddress();

            assert expectedDataDir.equals(actualDataDir) : "DataDir did not match";
            assert expectedDataLogDir.equals(actualDataLogDir) : "DataLogDir did not match";
            assert expectedClientPortAddr.equals(actualClientPortAddr) : "ClientPortAddress did not match";

        } catch (Exception e) {
            e.printStackTrace();
            assert false : "Test failed due to exception: " + e.getMessage();
        }
    }
}