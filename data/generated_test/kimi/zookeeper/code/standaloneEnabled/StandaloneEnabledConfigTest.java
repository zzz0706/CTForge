package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class StandaloneEnabledConfigTest {

    @Before
    public void setUp() {
        // Reset to default before each test
        QuorumPeerConfig.setStandaloneEnabled(true);
    }

    @After
    public void tearDown() {
        // Reset to default after each test
        QuorumPeerConfig.setStandaloneEnabled(true);
    }

    @Test
    public void testStandaloneEnabledDefaultValue() {
        // 1. Obtain configuration value via API
        boolean standaloneEnabled = QuorumPeerConfig.isStandaloneEnabled();
        
        // 2. Validate default value (should be true for backward compatibility)
        assertTrue("Default value of standaloneEnabled should be true", standaloneEnabled);
    }

    @Test
    public void testStandaloneEnabledValidTrue() {
        // 1. Set via API
        QuorumPeerConfig.setStandaloneEnabled(true);
        
        // 2. Obtain configuration value
        boolean standaloneEnabled = QuorumPeerConfig.isStandaloneEnabled();
        
        // 3. Validate boolean value
        assertTrue("standaloneEnabled should accept true", standaloneEnabled);
    }

    @Test
    public void testStandaloneEnabledValidFalse() {
        // 1. Set via API
        QuorumPeerConfig.setStandaloneEnabled(false);
        
        // 2. Obtain configuration value
        boolean standaloneEnabled = QuorumPeerConfig.isStandaloneEnabled();
        
        // 3. Validate boolean value
        assertFalse("standaloneEnabled should accept false", standaloneEnabled);
    }
}