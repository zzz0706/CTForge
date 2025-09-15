package org.apache.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestHadoopSecurityUidCacheSecs {

    /**
     * Test to validate the configuration value of `hadoop.security.uid.cache.secs`.
     * Ensures that the value satisfies the constraints and dependencies as outlined by the configuration's usage.
     */
    @Test
    public void testHadoopSecurityUidCacheSecsConfig() {
        // Load the configuration
        Configuration conf = new Configuration();
        
        // Retrieve the value of the configuration
        long cacheTimeoutSecs = conf.getLong(
            "hadoop.security.uid.cache.secs",
            14400 // Default value as per the configuration
        );

        // Step 1: Validate constraints
        // The `hadoop.security.uid.cache.secs` specifies the timeout in seconds.
        // Constraints:
        // 1. Should be a non-negative integer, as timeout cannot be negative or zero.
        // 2. Should be an integer (no floating-point values or other formats).

        // Validate the value
        assertTrue("Configuration `hadoop.security.uid.cache.secs` should have a non-negative timeout value.",
                cacheTimeoutSecs > 0);

        assertEquals(14400, (long) cacheTimeoutSecs);

    }
}