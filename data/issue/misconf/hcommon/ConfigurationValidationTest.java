package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;
//hadoop-4212 HADOOP-6578
public class ConfigurationValidationTest {

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

        // Step 3: Retrieve the raw string value to check for spaces
        String rawValue = conf.get(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY
        );
        if (rawValue != null) {
            // Check for leading or trailing spaces
            assertEquals(
                    "The configuration value must not contain leading or trailing whitespace.",
                    rawValue.trim(),
                    rawValue
            );
            // Check for inner spaces (if not allowed, optional)
            assertFalse(
                    "The configuration value must not contain whitespace characters.",
                    rawValue.contains(" ")
            );
        }

        // Step 4: Validate the value of 'hadoop.security.dns.log-slow-lookups.threshold.ms'
        if (logSlowLookupsEnabled) {
            // If logging slow lookups is enabled, threshold must be valid (positive integer)
            assertTrue("The configuration hadoop.security.dns.log-slow-lookups.threshold.ms must be a positive integer.",
                    slowLookupThresholdMs > 0);
        } else {
            // If logging slow lookups is disabled, the threshold value is irrelevant
            assertTrue("Threshold is non-impactful when slow lookup logging is disabled.", true);
        }
    }
}
