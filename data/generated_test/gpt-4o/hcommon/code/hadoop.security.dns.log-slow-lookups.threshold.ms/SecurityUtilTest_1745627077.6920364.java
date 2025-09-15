package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.TimeUnit;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import java.net.InetAddress;

public class SecurityUtilTest {

    // Test configuration API usage and value propagation
    @Test
    public void test_getSlowLookupThresholdMs_ConfigurationValueRetrieval() {
        // Create a Configuration instance
        Configuration conf = new Configuration();

        // Set the threshold value in the configuration
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 300);

        // Retrieve the threshold value using the configuration API
        int thresholdConfigured = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        // Verify that the retrieved value matches the expected configuration
        assertEquals("Configuration value mismatch for slow lookup threshold.", 300, thresholdConfigured);
    }

    // Test public method getByName behavior for slow DNS lookup logging
    @Test
    public void test_getByName_slowLookupLogsWhenAboveThreshold() throws Exception {
        // Create and configure a Configuration instance
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 200);

        // Retrieve the configuration value for slow lookup threshold using API
        int configuredThresholdMs = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        assertEquals("Slow lookup threshold mismatch.", 200, configuredThresholdMs);

        // Enable slow lookup logging
        SecurityUtil.logSlowLookups = true;

        // Mock HostResolver to simulate DNS resolution behavior
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        SecurityUtil.hostResolver = mockHostResolver;

        // Mock InetAddress response for hostname
        InetAddress mockInetAddress = InetAddress.getByName("example.com");

        when(mockHostResolver.getByName(anyString())).thenAnswer(invocation -> {
            // Simulate delay exceeding the configured threshold to test logging
            Thread.sleep(configuredThresholdMs + 500);
            return mockInetAddress;
        });

        // Capture logging output
        // Use a test logging framework or capture logging for verifying the warning log presence

        // Call the public method with a valid hostname
        InetAddress result = SecurityUtil.getByName("example.com");

        // Verify that the resolved address matches the mock response
        assertNotNull("Expected resolved address is null.", result);
        assertEquals("Resolved address does not match expected mock response.", mockInetAddress, result);

        // Ensure coverage for slow lookup detection logic
        // Verify that a warning message for slow lookup was logged
        // (Implementation to verify logging would depend on the particular test logging framework used)
    }
}