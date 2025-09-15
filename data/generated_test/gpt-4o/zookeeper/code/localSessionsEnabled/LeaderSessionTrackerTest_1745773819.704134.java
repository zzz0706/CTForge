package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.SessionTracker;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.apache.zookeeper.server.quorum.LeaderSessionTracker;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LeaderSessionTrackerTest {

    @Test
    // test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_createSession_withLocalSessionsEnabled() {
        // Step 1: Use API correctly to obtain configuration values
        QuorumPeerConfig configMock = Mockito.mock(QuorumPeerConfig.class);
        Mockito.when(configMock.areLocalSessionsEnabled()).thenReturn(true);

        // Step 2: Prepare test conditions
        boolean localSessionsEnabled = configMock.areLocalSessionsEnabled();

        ZooKeeperServerListener mockListener = new ZooKeeperServerListener() {
            @Override
            public void notifyStopping(String service, int exitCode) {
                // Mock listener behavior for testing
            }
        };

        ConcurrentMap<Long, Integer> sessionsWithTimeouts = new ConcurrentHashMap<>();

        SessionTracker.SessionExpirer expirerMock = new SessionTracker.SessionExpirer() {
            @Override
            public void expire(SessionTracker.Session session) {
                // Mock expiration logic
            }

            @Override
            public long getServerId() {
                return 1L; // Example server ID
            }
        };

        // Initialize LeaderSessionTracker with localSessionsEnabled set to true
        LeaderSessionTracker leaderSessionTracker = new LeaderSessionTracker(
                expirerMock,
                sessionsWithTimeouts,
                3000,  // tickTime from configuration (example value)
                expirerMock.getServerId(),
                localSessionsEnabled,
                mockListener
        );

        // Step 3: Test code - create session
        int sessionTimeout = 2000; // Example timeout value
        long sessionId = leaderSessionTracker.createSession(sessionTimeout);

        // Step 4: Verify results after testing
        boolean isLocalSession = !leaderSessionTracker.isGlobalSession(sessionId);
        assert isLocalSession : "Session ID should correspond to a local session.";
    }
}