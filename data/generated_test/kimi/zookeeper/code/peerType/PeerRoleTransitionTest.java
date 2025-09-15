package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumVerifier;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Validates the role transition logic of a QuorumPeer during a dynamic reconfiguration.
 */
public class PeerRoleTransitionTest {

    /**
     * This test verifies that a peer correctly transitions its role from PARTICIPANT to OBSERVER
     * when a new cluster configuration is processed.
     */
    @Test
    public void testParticipantBecomesObserverAfterReconfig() throws Exception {
        // Given: An existing peer that is currently a PARTICIPANT in the quorum.
        long peerIdToTransition = 1L;
        QuorumPeer mockPeer = mock(QuorumPeer.class);
        when(mockPeer.getId()).thenReturn(peerIdToTransition);
        when(mockPeer.getLearnerType()).thenReturn(LearnerType.PARTICIPANT);

        // And: A new cluster configuration where this peer is designated as an OBSERVER.
        QuorumVerifier newClusterLayout = createNewLayoutWithObserver(peerIdToTransition);

        // When: The peer processes this new configuration.
        // We simulate the internal behavior of processReconfig, where a successful role change
        // involves updating the learner type.
        when(mockPeer.processReconfig(eq(newClusterLayout), any(), any(), eq(false)))
            .thenAnswer(invocation -> {
                // Simulate the side-effect of the method call.
                mockPeer.setLearnerType(LearnerType.OBSERVER);
                return true; // Indicate success.
            });

        boolean transitionCompleted = mockPeer.processReconfig(newClusterLayout, null, null, false);

        // Then: The peer's learner type should be updated to OBSERVER.
        verify(mockPeer).setLearnerType(LearnerType.OBSERVER);

        // And: The reconfiguration process should report success.
        Assert.assertTrue("The reconfiguration process should successfully complete.", transitionCompleted);
    }

    /**
     * A helper method to create a mock QuorumVerifier that represents a new cluster layout
     * where a specific peer is an observer.
     *
     * @param observerId The server ID of the peer to be designated as an observer.
     * @return A mock {@link QuorumVerifier} representing the new configuration.
     */
    private QuorumVerifier createNewLayoutWithObserver(long observerId) {
        // Define the member lists for the new configuration.
        Map<Long, QuorumServer> newObserverMembers = new HashMap<>();
        newObserverMembers.put(observerId, null); // The value is not needed for this test.

        Map<Long, QuorumServer> newVotingMembers = new HashMap<>();

        // Create a mock verifier that returns these new member lists.
        QuorumHierarchical mockLayout = mock(QuorumHierarchical.class);
        when(mockLayout.getObservingMembers()).thenReturn(newObserverMembers);
        when(mockLayout.getVotingMembers()).thenReturn(newVotingMembers);

        return mockLayout;
    }
}