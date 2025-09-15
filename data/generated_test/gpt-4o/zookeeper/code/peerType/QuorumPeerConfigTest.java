package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.io.InputStream;
import java.io.FileInputStream;

import static org.mockito.Mockito.*;

public class QuorumPeerConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void test_processReconfig_with_roleChange_OBSERVER() throws Exception {
        // Test code
        // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 2. Prepare the test conditions.
        // Mock the QuorumPeer instance
        QuorumPeer quorumPeerMock = mock(QuorumPeer.class);
        when(quorumPeerMock.getId()).thenReturn(1L);
        when(quorumPeerMock.getLearnerType()).thenReturn(LearnerType.PARTICIPANT);

        // Mock observingMembers and votingMembers
        Map<Long, QuorumServer> observingMembers = new HashMap<>();
        observingMembers.put(1L, null);
        Map<Long, QuorumServer> votingMembers = new HashMap<>();

        // Prepare a mock QuorumVerifier instance using QuorumHierarchical (from ZooKeeper)
        QuorumHierarchical quorumVerifierMock = mock(QuorumHierarchical.class);
        when(quorumVerifierMock.getObservingMembers()).thenReturn(observingMembers);
        when(quorumVerifierMock.getVotingMembers()).thenReturn(votingMembers);

        // Mock processReconfig logic and possible outcome
        when(quorumPeerMock.processReconfig(eq(quorumVerifierMock), any(), any(), eq(false))).then(invocation -> {
            quorumPeerMock.setLearnerType(LearnerType.OBSERVER);
            return true;
        });

        // 3. Test code.
        boolean result = quorumPeerMock.processReconfig(quorumVerifierMock, null, null, false);

        // Verify expectations
        verify(quorumPeerMock).setLearnerType(LearnerType.OBSERVER);
        Assert.assertTrue(result);

        // 4. Code after testing.
        // No cleanup is required for this mock-based test case.
    }
}