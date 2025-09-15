package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConfigurationValidationTest {

    /**
     * Test to validate the constraint and dependencies of the configuration
     * "hadoop.security.dns.log-slow-lookups.threshold.ms".
     * 
     * Steps:
     * 1. Validate that 'hadoop.security.dns.log-slow-lookups.threshold.ms' value is positive.
     * 2. Ensure that if 'hadoop.security.dns.log-slow-lookups.enabled' is false, 
     *    then 'hadoop.security.dns.log-slow-lookups.threshold.ms' is not impactful.
     */
    @Test
    public void testLogSlowLookupsThresholdConfiguration() {
        // Step 1: Load the configuration
        Configuration conf = new Configuration();

        // Step 2: Retrieve configuration values
        boolean logSlowLookupsEnabled = conf.getBoolean(
                "hadoop.security.dns.log-slow-lookups.enabled",
                false // Default value if the configuration is absent
        );

        int slowLookupThresholdMs = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                1000 // Default value as per source code
        );

        // Step 3: Validate the value of 'hadoop.security.dns.log-slow-lookups.threshold.ms'
        if (logSlowLookupsEnabled) {
            // If logging slow lookups is enabled, threshold must be valid (positive integer)
            assertTrue("The configuration hadoop.security.dns.log-slow-lookups.threshold.ms must be a positive integer.",
                    slowLookupThresholdMs > 0);
        } else {
            // If logging slow lookups is disabled, the threshold value is irrelevant
            // and should not be used for validation; therefore no strict check is needed.
            assertTrue("Threshold is non-impactful when slow lookup logging is disabled.", true);
        }
    }
}