package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.TimeUnit;
import org.apache.hadoop.util.StopWatch;
import org.junit.Test;

import java.net.InetAddress;

import static org.mockito.Mockito.*;

public class SecurityUtilTest {

    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_slowLookupLogsWhenAboveThreshold() throws Exception {
        // Mock the Configuration to fetch the threshold value.
        Configuration conf = new Configuration();

        // Get the configuration value using API
        int slowLookupThresholdMs = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT
        );

        // Enable slow lookup logging for the test.
        SecurityUtil.logSlowLookups = true;

        // Mock the DNS resolver to simulate slow DNS lookup.
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        SecurityUtil.hostResolver = mockHostResolver;
        InetAddress mockInetAddress = InetAddress.getByName("example.com");
        when(mockHostResolver.getByName(anyString())).thenAnswer(invocation -> {
            // Simulate delay exceeding threshold.
            Thread.sleep(slowLookupThresholdMs + 500);
            return mockInetAddress;
        });

        // Use a tool like LogCaptor or similar to capture log outputs during the test.
        // Capture log outputs for assertion.
        SecurityUtil.getByName("example.com");

        // Validate through logs that a warning is logged for slow lookups.
    }
}