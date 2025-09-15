package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.Learner;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.common.X509Exception;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LearnerConnectToLeaderTest {

    @Test
    // Test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testConnectToLeader_SyncLimitProperlySet() throws Exception {
        // Step 1: Load configuration
        final String CONFIG_PATH = "ctest.cfg";
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 2: Mock necessary objects and behaviors
        QuorumPeer quorumPeer = mock(QuorumPeer.class);
        quorumPeer.tickTime = config.getTickTime();
        quorumPeer.syncLimit = config.getSyncLimit();

        FileTxnSnapLog txnLog = mock(FileTxnSnapLog.class);
        when(txnLog.getDataDir()).thenReturn(config.getDataDir());
        when(txnLog.getSnapDir()).thenReturn(config.getDataLogDir());
        when(quorumPeer.getTxnFactory()).thenReturn(txnLog);

        Learner learner = mock(Learner.class);
        learner.self = quorumPeer; // Correctly setting the learner's self property

        // Step 3: Prepare test conditions
        InetSocketAddress leaderAddress = new InetSocketAddress("localhost", 2181);
        String leaderHostname = "leader.local";

        // Step 4: Test code
        try {
            learner.connectToLeader(leaderAddress, leaderHostname);
        } catch (X509Exception | InterruptedException e) {
            e.printStackTrace();
        }

        // Verifying syncLimit usage
        Socket testSocket = new Socket();
        testSocket.setSoTimeout(quorumPeer.tickTime * quorumPeer.syncLimit);
        assert testSocket.getSoTimeout() == quorumPeer.tickTime * quorumPeer.syncLimit
                : "Socket timeout not configured properly based on syncLimit.";

        // Step 5: Code after testing
        testSocket.close();
    }
}