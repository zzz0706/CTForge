package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.apache.zookeeper.server.quorum.LeaderSessionTracker;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertTrue;

public class LeaderSessionTrackerTest {

    @Test
    // Test code
    // 1. Correctly use the zookeeper3.5.6 API to obtain configuration values, and avoid hardcoding configuration values.
    // 2. Prepare the test conditions.
    // 3. Test execution.
    // 4. Verify the expected behavior.
    public void test_addGlobalSession_withLocalSessionsEnabled() {
        // Preparing test conditions
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.areLocalSessionsEnabled()).thenReturn(true); // Correctly get configuration value via API

        ZooKeeperServerListener serverListenerMock = mock(ZooKeeperServerListener.class);
        ConcurrentHashMap<Long, Integer> sessionsWithTimeouts = new ConcurrentHashMap<>();

        // Note the correction of the second parameter from ZooKeeperServerListener to SessionExpirer
        LeaderSessionTracker leaderSessionTracker = new LeaderSessionTracker(serverListenerMock, sessionsWithTimeouts, 2000, 1L, configMock.areLocalSessionsEnabled(), mock(SessionExpirer.class));

        // Test execution
        long sessionId = 12345L; // Mock session ID
        int sessionTimeout = 30000; // Mock session timeout
        boolean result = leaderSessionTracker.addGlobalSession(sessionId, sessionTimeout);

        // Verify the expected behavior
        assertTrue("Global session should be added successfully when localSessionsEnabled is true.", result);
    }
}