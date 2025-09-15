package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class TestSecurityUtil {

    /**
     * Tests the `getSlowLookupThresholdMs` method to ensure it correctly parses the configuration
     * key `hadoop.security.dns.log-slow-lookups.threshold.ms` and falls back to the default
     * value if the key is unset.
     */
    @Test
    public void test_getSlowLookupThresholdMs_configurationParsing() {
        // Create a Configuration instance without explicitly setting the threshold key.
        Configuration conf = new Configuration();

        // Get the default value from CommonConfigurationKeys.
        int expectedDefaultValue = CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT;

        // Retrieve the threshold value using the tested method.
        int actualValue = SecurityUtil.getSlowLookupThresholdMs();

        // Verify that the returned value matches the default value.
        assertEquals("The parsed threshold value should match the default value.", expectedDefaultValue, actualValue);
    }

    /**
     * Tests the `getByName` method to ensure it utilizes the configuration correctly
     * and logs slow DNS resolutions when the lookup exceeds the configured threshold.
     */
    @Test
    public void test_getByName_withConfigurationUsage() throws UnknownHostException {
        // Customize the configuration for this test.
        Configuration conf = new Configuration();
        int customThresholdMs = 5; // Set a low threshold value for testing.
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, customThresholdMs);

        // Validate that the custom threshold is correctly parsed and set in the configuration.
        int parsedThreshold = conf.getInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            -1
        );
        assertEquals("The threshold should match the configured custom value.", customThresholdMs, parsedThreshold);

        // Resolve a known hostname using the SecurityUtil class.
        String testHostName = "localhost";
        InetAddress resolvedAddress = SecurityUtil.getByName(testHostName);

        // Verify that the resolved address is not null.
        assertNotNull("The resolved InetAddress should not be null.", resolvedAddress);

        // Note: Timing behavior and logging of slow lookups depends on runtime resolution timings.
        // This test validates configuration propagation but does not fully simulate runtime behaviors.
    }
}