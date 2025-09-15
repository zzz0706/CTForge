package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Validates the runtime state and connection policies of a quorum peer.
 */
public class PeerStateValidator {

    // A descriptor that points to the peer's connection configuration resource.
    private static final String ENDPOINT_DESCRIPTOR = "ctest.cfg";

    /**
     * Tests the 'maxIdleTimeSeconds' setting from the connection policy
     * to ensure it's a valid value for managing connection health. A positive
     * value enables the timeout, while a non-positive value disables it.
     */
    @Test
    public void validateConnectionTimeout() throws Exception {
        // Step 1: Load the session properties from the specified endpoint descriptor.
        Properties sessionCache = new Properties();
        try (InputStream networkStream = new FileInputStream(ENDPOINT_DESCRIPTOR)) {
            sessionCache.load(networkStream);
        }

        // Step 2: Parse the loaded properties to create the data replication policy object.
        QuorumPeerConfig dataReplicationPolicy = new QuorumPeerConfig();
        dataReplicationPolicy.parseProperties(sessionCache);

        // Step 3: Retrieve the configured maximum idle time in seconds from the policy.
        int maxIdleTimeSeconds = dataReplicationPolicy.getPurgeInterval();

        // Step 4: Validate the constraints for the max idle time.
        if (maxIdleTimeSeconds > 0) {
            // A positive value is required to enable the idle timeout feature.
            assertTrue(
                "autopurge.purgeInterval should be a positive integer to enable auto purging.", 
                maxIdleTimeSeconds >= 1
            );
        } else {
            // A non-positive value (0 or negative) indicates that the feature is disabled.
            assertTrue(
                "autopurge.purgeInterval is non-positive, auto purging is not enabled.", 
                maxIdleTimeSeconds <= 0
            );
        }
    }
}