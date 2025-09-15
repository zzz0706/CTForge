package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper.server.quorum.LeaderSessionTracker;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.mock;

/**
 * Test class for verifying the behavior of LeaderSessionTracker with localSessionsEnabled disabled.
 */
public class LeaderSessionTrackerTest {

    @Test
    // Test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_addGlobalSession_withLocalSessionsDisabled() {
        // Step 1: Set up mock dependencies
        SessionExpirer mockExpirer = mock(SessionExpirer.class);
        ConcurrentMap<Long, Integer> sessionsWithTimeouts = new ConcurrentHashMap<>();

        // Step 2: Retrieve configuration value using the API without hardcoding
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        Mockito.when(configMock.areLocalSessionsEnabled()).thenReturn(false);

        // Step 3: Initialize LeaderSessionTracker with localSessionsEnabled set to false using the config mock
        LeaderSessionTracker leaderSessionTracker = new LeaderSessionTracker(
                mockExpirer,
                sessionsWithTimeouts,
                2000, // Assuming tickTime as 2000 for testing purposes
                1L,   // Mock server ID
                configMock.areLocalSessionsEnabled(),
                null  // ZooKeeperServerListener is optional for this test
        );

        // Step 4: Execute the method to test
        long mockSessionId = 0x12345678L; // Mock session ID
        int mockSessionTimeout = 5000;   // Mock session timeout
        boolean result = leaderSessionTracker.addGlobalSession(mockSessionId, mockSessionTimeout);

        // Step 5: Verify expected behavior through assertions
        assert result : "Global session was not added when localSessionsEnabled was disabled.";
        assert sessionsWithTimeouts.containsKey(mockSessionId) : "Session ID should have been added to the sessionsWithTimeouts map.";
    }
}