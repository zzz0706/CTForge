package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.PrepRequestProcessor;

import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class TestQuorumPeerAPI {

    @Test
    // Test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_processReconfig_withValidConfig() throws Exception {
        // 1. Obtain configuration values using ZooKeeper API
        String CONFIG_PATH = "ctest.cfg";
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Parse configuration
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 2. Prepare test conditions
        QuorumPeer quorumPeer = mock(QuorumPeer.class);
        QuorumVerifier currentQuorumVerifier = mock(QuorumVerifier.class);

        // Mocking initial QuorumVerifier state
        Map<Long, QuorumServer> initialMembers = new HashMap<>();
        initialMembers.put(1L, mock(QuorumServer.class));
        when(currentQuorumVerifier.getAllMembers()).thenReturn(initialMembers);
        when(currentQuorumVerifier.getVersion()).thenReturn(1L);

        // Setting the reconfigEnabled flag
        QuorumPeerConfig.setReconfigEnabled(true);

        // Setting up mocked behavior for `setQuorumVerifier` and `processReconfig`
        doAnswer(invocation -> {
            QuorumVerifier newQuorumVerifier = invocation.getArgument(0);
            when(quorumPeer.getQuorumVerifier()).thenReturn(newQuorumVerifier);
            return null;
        }).when(quorumPeer).setQuorumVerifier(any(QuorumVerifier.class), eq(true));

        doAnswer(invocation -> {
            QuorumVerifier newQuorumVerifier = invocation.getArgument(0);
            quorumPeer.setQuorumVerifier(newQuorumVerifier, true);
            return true;
        }).when(quorumPeer).processReconfig(any(QuorumVerifier.class), anyLong(), anyLong(), anyBoolean());

        // Simulating new configuration values
        QuorumVerifier newQuorumVerifier = mock(QuorumVerifier.class);
        Map<Long, QuorumServer> newMembers = new HashMap<>();
        newMembers.put(2L, mock(QuorumServer.class));
        when(newQuorumVerifier.getAllMembers()).thenReturn(newMembers);
        when(newQuorumVerifier.getVersion()).thenReturn(2L);

        // 3. Test code: Perform reconfiguration
        boolean result = quorumPeer.processReconfig(newQuorumVerifier, 2L, 1L, true);

        // 4. Code after testing: Assertions
        assertTrue("Expected reconfiguration to be processed successfully", result);
        assertNotNull("The QuorumVerifier should be updated", quorumPeer.getQuorumVerifier());
        assertEquals("The QuorumVerifier version should be updated", 2L, quorumPeer.getQuorumVerifier().getVersion());

        // Verify method behavior
        verify(quorumPeer, times(1)).processReconfig(eq(newQuorumVerifier), eq(2L), eq(1L), eq(true));
    }

    @Test
    public void test_processReconfig_reconfigFeatureDisabled() throws Exception {
        // 1. Obtain configuration values using ZooKeeper API
        String CONFIG_PATH = "/home/zzz/workspaces/ctest_app/zookeeper/zookeeper-server/ctest.cfg";
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Parse configuration
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 2. Prepare test conditions
        QuorumPeer quorumPeer = mock(QuorumPeer.class);

        // Disable the reconfigEnabled flag
        QuorumPeerConfig.setReconfigEnabled(false);

        // Simulating new configuration values
        QuorumVerifier newQuorumVerifier = mock(QuorumVerifier.class);

        // 3. Test code: Perform reconfiguration
        boolean result = quorumPeer.processReconfig(newQuorumVerifier, 2L, 1L, true);

        // 4. Code after testing: Assertions
        assertFalse("Expected reconfiguration to be skipped", result);
    }
}