package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.apache.zookeeper.server.SessionTracker;
import org.apache.zookeeper.server.quorum.LeaderSessionTracker;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;

public class LeaderSessionTrackerTest {

    @Test
    // Test: Verify that a session is correctly handled when a local session is upgraded to a global session.
    // 1. Use the Zookeeper 3.5.6 API properly to obtain configuration values, instead of hardcoding configuration values.
    // 2. Prepare the test conditions.
    // 3. Write the test code to confirm the behavior.
    // 4. Validate the test results and conclude the logic.
    public void test_addSession_withLocalSessionEnabled_GlobalSessionUpgrade() {
        // Step 1: Set up mock SessionTracker.SessionExpirer and ConcurrentMap sessionsWithTimeouts
        SessionTracker.SessionExpirer mockExpirer = Mockito.mock(SessionTracker.SessionExpirer.class);
        ConcurrentHashMap<Long, Integer> sessionsWithTimeouts = new ConcurrentHashMap<>();

        // Step 2: Use ZooKeeperServerListener mock
        ZooKeeperServerListener mockListener = Mockito.mock(ZooKeeperServerListener.class);

        // Step 3: Create a LeaderSessionTracker instance using mocked configurations
        long serverId = 1L; // mock serverId
        int tickTime = 3000; // mock tickTime

        LeaderSessionTracker leaderSessionTracker = new LeaderSessionTracker(
            mockExpirer, sessionsWithTimeouts, tickTime, serverId, true /* localSessionsEnabled */, mockListener
        );

        // Step 4: Prepare data for testing the behavior of session tracking
        long sessionId = 12345L; // mock sessionId
        int sessionTimeout = 5000; // mock sessionTimeout

        // Step 5: Add a session as a local session using the `addSession` method
        boolean isAddedAsLocalSession = leaderSessionTracker.addSession(sessionId, sessionTimeout);
        assert isAddedAsLocalSession; // Ensure the session was added locally

        // Step 6: Simulate a condition where the session is upgraded to a global session
        boolean isGlobalSession = leaderSessionTracker.addGlobalSession(sessionId, sessionTimeout);
        assert isGlobalSession; // Ensure the session is now upgraded to a global session

        // Step 7: Validate internal state of the session tracking
        Integer storedTimeout = sessionsWithTimeouts.get(sessionId);
        assert storedTimeout == sessionTimeout; // Ensure the timeout value is tracked correctly

        // Test clean-up and ensure no additional resources need to be released, as LeaderSessionTracker handles lifecycle automatically.
    }
}