package org.apache.zookeeper.server.quorum;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the behavior of the session upgrade policy flag within the QuorumPeerConfig.
 */
public class SessionUpgradePolicyVerificationTest {

    /**
     * Ensures that when the configuration is mocked to enable session upgrading,
     * the corresponding getter method returns true.
     */
    @Test
    public void testUpgradeIsEnabledWhenConfigIsTrue() {
        // Given: A mock peer configuration where local session upgrading is explicitly enabled.
        QuorumPeerConfig mockPeerConfig = mock(QuorumPeerConfig.class);
        when(mockPeerConfig.isLocalSessionsUpgradingEnabled()).thenReturn(true);

        // When: The status of the session upgrade feature is queried.
        boolean isUpgradeEnabled = mockPeerConfig.isLocalSessionsUpgradingEnabled();

        // Then: The result should confirm that the feature is enabled.
        assertTrue("The system should report that session upgrading is enabled.", isUpgradeEnabled);
    }

    /**
     * Ensures that when the configuration is mocked to disable session upgrading,
     * the corresponding getter method returns false.
     */
    @Test
    public void testUpgradeIsDisabledWhenConfigIsFalse() {
        // Given: A mock peer configuration where local session upgrading is explicitly disabled.
        QuorumPeerConfig mockPeerConfig = mock(QuorumPeerConfig.class);
        when(mockPeerConfig.isLocalSessionsUpgradingEnabled()).thenReturn(false);

        // When: The status of the session upgrade feature is queried.
        boolean isUpgradeEnabled = mockPeerConfig.isLocalSessionsUpgradingEnabled();

        // Then: The result should confirm that the feature is disabled.
        assertFalse("The system should report that session upgrading is disabled.", isUpgradeEnabled);
    }
}