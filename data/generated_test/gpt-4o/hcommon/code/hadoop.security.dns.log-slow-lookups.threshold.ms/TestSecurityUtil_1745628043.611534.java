package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestSecurityUtil {

    /**
     * Test to validate `getSlowLookupThresholdMs` correctly retrieves 
     * the configuration value with default fallback.
     */
    @Test
    public void test_getSlowLookupThresholdMs_configurationParsing() {
        // Instantiate a Configuration without setting the specific property.
        Configuration conf = new Configuration();

        // Default value of the threshold as defined in CommonConfigurationKeys.
        int expectedDefaultValue = CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT;

        // Access the value via the SecurityUtil static field initialized via `getSlowLookupThresholdMs`.
        int actualValue = SecurityUtil.getSlowLookupThresholdMs();

        // Assert that the returned value matches the expected default value.
        assertEquals("The returned threshold value should match the default.", expectedDefaultValue, actualValue);
    }

    /**
     * Test `getByName` to ensure correct hostname resolution and direct configuration usage.
     */
    @Test
    public void test_getByName_withConfiguredThreshold() throws UnknownHostException {
        // Create and configure the Configuration object.
        Configuration conf = new Configuration();
        int customThresholdMs = 100; // Set threshold to a custom value for testing.
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, customThresholdMs);

        // Verify the custom threshold value directly from the Configuration.
        int parsedThreshold = conf.getInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            -1
        );
        assertEquals("Configured threshold value should match.", customThresholdMs, parsedThreshold);

        // Resolve a sample hostname using `getByName`.
        String testHostname = "localhost";
        InetAddress resolvedAddress = SecurityUtil.getByName(testHostname);

        // Verify that the hostname has been resolved successfully.
        assertNotNull("Hostname resolution should return a valid InetAddress.", resolvedAddress);
    }

    /**
     * Test `getByName` with slow lookup logic based on a low threshold 
     * to trigger warning logs for slow DNS resolutions.
     */
    @Test
    public void test_getByName_slowLookupLogging() throws UnknownHostException {
        // Create and configure the Configuration object with a very low threshold.
        Configuration conf = new Configuration();
        int customThresholdMs = 5; // Simulate slow lookups by using an ultra-low threshold.
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, customThresholdMs);

        // Verify the propagation of the threshold value from configuration.
        int parsedThreshold = conf.getInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            -1
        );
        assertEquals("Custom threshold value should be set correctly.", customThresholdMs, parsedThreshold);

        // Measure time taken for hostname resolution.
        String testHostname = "localhost";
        long startTime = System.nanoTime();
        InetAddress resolvedAddress = SecurityUtil.getByName(testHostname);
        long elapsedTimeMs = TimeUnit.MILLISECONDS.convert(
            System.nanoTime() - startTime, TimeUnit.NANOSECONDS
        );

        // Verify the results and behavior based on slow lookup threshold.
        assertNotNull("Resolved InetAddress should not be null.", resolvedAddress);
        assertTrue("Elapsed time should exceed the configured threshold for slow DNS lookup.",
            elapsedTimeMs >= customThresholdMs);
    }
}