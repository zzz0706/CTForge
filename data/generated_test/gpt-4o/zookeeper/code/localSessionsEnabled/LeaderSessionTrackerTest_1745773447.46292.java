package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.apache.zookeeper.server.quorum.LeaderSessionTracker;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.Request;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class LeaderSessionTrackerTest {

    @Test
    // Test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test execution.
    // 4. Code after testing.
    public void test_addGlobalSession_withLocalSessionsEnabled() {
        // 1. Use zookeeper3.5.6 API correctly to obtain configuration values
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.areLocalSessionsEnabled()).thenReturn(true);

        // 2. Prepare test conditions
        ZooKeeperServerListener serverListenerMock = mock(ZooKeeperServerListener.class);
        ConcurrentHashMap<Long, Integer> sessionsWithTimeouts = new ConcurrentHashMap<>();
        SessionExpirer expirerMock = mock(SessionExpirer.class);

        LeaderSessionTracker leaderSessionTracker = new LeaderSessionTracker(
                expirerMock, sessionsWithTimeouts, 2000, 1L, configMock.areLocalSessionsEnabled(), serverListenerMock);

        // 3. Test execution
        long sessionId = 12345L; // Mock session ID
        int sessionTimeout = 30000; // Mock session timeout
        boolean result = leaderSessionTracker.addGlobalSession(sessionId, sessionTimeout);

        // 4. Verify expected behavior
        assertTrue("Global session should be added successfully when localSessionsEnabled is true.", result);
    }

    @Test
    // Test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test execution.
    // 4. Code after testing.
    public void test_addSession_withLocalSessionsEnabled() {
        // 1. Use zookeeper3.5.6 API correctly to obtain configuration values
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.areLocalSessionsEnabled()).thenReturn(true);

        // 2. Prepare test conditions
        ZooKeeperServerListener serverListenerMock = mock(ZooKeeperServerListener.class);
        ConcurrentHashMap<Long, Integer> sessionsWithTimeouts = new ConcurrentHashMap<>();
        SessionExpirer expirerMock = mock(SessionExpirer.class);

        LeaderSessionTracker leaderSessionTracker = new LeaderSessionTracker(
                expirerMock, sessionsWithTimeouts, 2000, 1L, configMock.areLocalSessionsEnabled(), serverListenerMock);

        // 3. Test execution
        long sessionId = 67890L; // Mock local session ID
        int sessionTimeout = 15000; // Mock session timeout
        boolean result = leaderSessionTracker.addSession(sessionId, sessionTimeout);

        // 4. Verify expected behavior
        assertTrue("Local session should be added successfully when localSessionsEnabled is true.", result);
    }

    @Test
    // Test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test execution.
    // 4. Code after testing.
    public void test_createSession_withLocalSessionsEnabled() {
        // 1. Use zookeeper3.5.6 API correctly to obtain configuration values
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.areLocalSessionsEnabled()).thenReturn(true);

        // 2. Prepare test conditions
        ZooKeeperServerListener serverListenerMock = mock(ZooKeeperServerListener.class);
        ConcurrentHashMap<Long, Integer> sessionsWithTimeouts = new ConcurrentHashMap<>();
        SessionExpirer expirerMock = mock(SessionExpirer.class);

        LeaderSessionTracker leaderSessionTracker = new LeaderSessionTracker(
                expirerMock, sessionsWithTimeouts, 2000, 1L, configMock.areLocalSessionsEnabled(), serverListenerMock);

        // 3. Test execution
        int sessionTimeout = 45000; // Mock session timeout
        long sessionId = leaderSessionTracker.createSession(sessionTimeout);

        // 4. Verify expected behavior
        assertTrue("Session ID should be a positive number.", sessionId > 0);
    }
}