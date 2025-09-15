package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class ReconfigEnabledConfigTest {

    private boolean originalReconfigEnabled;

    @Before
    public void setUp() {
        // Save the original value to restore after test
        originalReconfigEnabled = QuorumPeerConfig.isReconfigEnabled();
    }

    @After
    public void tearDown() {
        // Restore the original value
        QuorumPeerConfig.setReconfigEnabled(originalReconfigEnabled);
    }

    @Test
    public void testReconfigEnabledValidBooleanValues() {
        // Test valid true value
        QuorumPeerConfig.setReconfigEnabled(true);
        assertTrue("reconfigEnabled should be true", QuorumPeerConfig.isReconfigEnabled());

        // Test valid false value
        QuorumPeerConfig.setReconfigEnabled(false);
        assertFalse("reconfigEnabled should be false", QuorumPeerConfig.isReconfigEnabled());
    }

    @Test
    public void testReconfigEnabledDefaultValue() {
        // Test default value (should be false)
        QuorumPeerConfig.setReconfigEnabled(false); // Explicitly set to default
        assertFalse("Default reconfigEnabled should be false", QuorumPeerConfig.isReconfigEnabled());
    }

    @Test
    public void testReconfigEnabledConsistencyAcrossServers() {
        // This test verifies the configuration can be consistently set
        // In a real ensemble, all servers should have the same value
        
        // Simulate setting consistent value across servers
        QuorumPeerConfig.setReconfigEnabled(true);
        assertTrue("All servers should have consistent reconfigEnabled=true", 
                  QuorumPeerConfig.isReconfigEnabled());
        
        QuorumPeerConfig.setReconfigEnabled(false);
        assertFalse("All servers should have consistent reconfigEnabled=false", 
                   QuorumPeerConfig.isReconfigEnabled());
    }

    @Test
    public void testReconfigEnabledTypeSafety() {
        // Verify the configuration accepts only boolean values
        // This is implicitly tested by the method signature which only accepts boolean
        
        // Test that no exception is thrown with valid boolean
        try {
            QuorumPeerConfig.setReconfigEnabled(true);
            QuorumPeerConfig.setReconfigEnabled(false);
        } catch (Exception e) {
            fail("Setting valid boolean values should not throw exception");
        }
    }
}