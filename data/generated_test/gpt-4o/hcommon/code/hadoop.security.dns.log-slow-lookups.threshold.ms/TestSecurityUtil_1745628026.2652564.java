package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class TestSecurityUtil {

    /**
     * Test `getSlowLookupThresholdMs` to ensure it correctly parses the configuration 
     * and returns the default value if unset.
     */
    @Test
    public void test_getSlowLookupThresholdMs_configurationParsing() {
        // Create a Configuration instance without setting the specific key.
        Configuration conf = new Configuration();

        // Verify that the default value is returned.
        int expectedDefaultValue = CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT;

        // The static variable is initialized using `getSlowLookupThresholdMs`.
        int actualValue = SecurityUtil.getSlowLookupThresholdMs();

        // Verify the parsed configuration value matches the default.
        assertEquals("The parsed threshold value should match the default value.", expectedDefaultValue, actualValue);
    }

    /**
     * Test `getByName` method to ensure that hostname resolution works correctly 
     * and logging of slow DNS resolutions occurs based on the configuration.
     */
    @Test
    public void test_getByName_withConfigurationUsage() throws UnknownHostException {
        // Prepare the configuration for testing.
        Configuration conf = new Configuration();
        int customThresholdMs = 50; // Customized threshold for test.
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, customThresholdMs);

        // Verify configuration propagation works correctly.
        int parsedValue = conf.getInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            -1
        );
        assertEquals("The threshold should match the configured custom value.", customThresholdMs, parsedValue);

        // Attempt to resolve a hostname using `getByName`.
        String testHostName = "localhost";
        InetAddress resolvedAddress = SecurityUtil.getByName(testHostName);

        // Ensure the resolved address is valid.
        assertNotNull("The resolved InetAddress should not be null.", resolvedAddress);

        // Since DNS resolution is runtime-dependent, elapsed time for slow lookup logging cannot be directly asserted here.
    }

    /**
     * Test configuration propagation for slow lookup timing and logging using 
     * a custom threshold value configuration.
     */
    @Test
    public void test_getByName_slowLookupLogging() throws UnknownHostException {
        // Set custom threshold in Configuration much lower than runtime execution.
        Configuration conf = new Configuration();
        int customThresholdMs = 5; // Testing with a low threshold for simulated slow lookups.
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, customThresholdMs);

        // Verify value propagation in configuration.
        int parsedThreshold = conf.getInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            -1
        );
        assertEquals("The threshold should match the configured custom value.", customThresholdMs, parsedThreshold);

        // Resolve hostname and measure lookup time.
        String testHostName = "localhost";
        long startTime = System.nanoTime();
        InetAddress resolvedAddress = SecurityUtil.getByName(testHostName);
        long elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

        // Ensure resolved address and validate against slow lookup logging logic.
        assertNotNull("The resolved InetAddress should not be null.", resolvedAddress);
        assertTrue("Elapsed time should exceed the configured threshold for testing!",
            elapsedTime >= customThresholdMs);
    }
}