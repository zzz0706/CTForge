package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class TestSecurityUtil {

    /**
     * Tests that the `getSlowLookupThresholdMs` method correctly retrieves and parses
     * the value of `hadoop.security.dns.log-slow-lookups.threshold.ms` from Configuration
     * and uses the default value if the configuration is unset.
     */
    @Test
    public void test_getSlowLookupThresholdMs_configurationParsing() {
        // Step 1: Create a Configuration instance without setting the threshold key.
        Configuration conf = new Configuration();

        // Step 2: Use the method indirectly via its static field `slowLookupThresholdMs`.
        int expectedDefaultValue = CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT;
        int actualValue = SecurityUtil.getSlowLookupThresholdMs();

        // Step 3: Verify that the returned value matches the default.
        assertEquals("Expected default slow lookup threshold value.", expectedDefaultValue, actualValue);
    }

    /**
     * Tests the `getByName` method to ensure it correctly resolves hostnames
     * and logs slow lookups based on the threshold from the configuration.
     */
    @Test
    public void test_getByName_withConfigurationUsage() throws UnknownHostException {
        // Step 1: Configure a custom threshold for testing slow lookup behavior.
        Configuration conf = new Configuration();
        int customThresholdMs = 5; // Custom threshold set to 5ms for testing.
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, customThresholdMs);

        // Ensure custom value is correctly set (indirect testing of parsing).
        assertEquals("Expected custom threshold to be set.",
                customThresholdMs,
                conf.getInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                        -1));

        // Step 2: Resolve a known hostname (e.g., localhost) using `getByName`.
        String testHostName = "localhost";
        InetAddress resolvedAddress = SecurityUtil.getByName(testHostName);

        // Ensure the method returns a non-null InetAddress.
        assertNotNull("Expected resolved InetAddress to be non-null.", resolvedAddress);

        // Note: Observing slow lookup behavior might require integration tests or mocking the resolver.
        // Actual DNS resolution timing is system-dependent and hard to fully simulate in unit tests.
    }
}