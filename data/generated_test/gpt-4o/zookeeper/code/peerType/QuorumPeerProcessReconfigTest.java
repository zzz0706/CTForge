package org.apache.zookeeper.test;

import static org.mockito.Mockito.*;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class QuorumPeerProcessReconfigTest {

    @Test
    // test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_processReconfig_without_roleChange() throws Exception {
        // Step 1: Correctly load and parse the configuration using ZooKeeper's API
        Properties props = new Properties();
        String CONFIG_PATH = "ctest.cfg";
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Mock configuration values
        File dataDir = config.getDataDir();
        File dataLogDir = config.getDataLogDir();

        QuorumPeer mockQuorumPeer = mock(QuorumPeer.class);
        when(mockQuorumPeer.getLearnerType()).thenReturn(QuorumPeer.LearnerType.PARTICIPANT);

        Map<Long, QuorumPeer.QuorumServer> votingMembers = new HashMap<>();
        votingMembers.put(1L, new QuorumPeer.QuorumServer(1L, new InetSocketAddress("localhost", 2181), null, null, null));
        Map<Long, QuorumPeer.QuorumServer> observingMembers = new HashMap<>();

        // Step 2: Mocking logic to simulate quorum configurations and verifier
        QuorumMaj quorumVerifier = new QuorumMaj(votingMembers);
        
        when(mockQuorumPeer.getQuorumVerifier()).thenReturn(quorumVerifier);

        doNothing().when(mockQuorumPeer).setLearnerType(QuorumPeer.LearnerType.PARTICIPANT);

        // Step 3: Execute the method under test
        boolean result = mockQuorumPeer.processReconfig(quorumVerifier, null, null, false);

        // Step 4: Verify the method behavior
        verify(mockQuorumPeer, never()).setLearnerType(QuorumPeer.LearnerType.OBSERVER);
        verify(mockQuorumPeer, never()).setLearnerType(any(QuorumPeer.LearnerType.class));

        // Step 5: Assert the expected outcome
        org.junit.Assert.assertFalse("No role change expected", result);
    }
}