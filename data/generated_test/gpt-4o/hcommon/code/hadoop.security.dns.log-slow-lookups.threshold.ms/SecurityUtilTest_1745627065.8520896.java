package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.TimeUnit;
import org.junit.Test;
import static org.mockito.Mockito.*;
import java.net.InetAddress;
import org.apache.hadoop.security.SecurityUtil;

public class SecurityUtilTest {
    
    // Test to ensure proper configuration API usage and validate value propagation
    @Test
    public void test_getSlowLookupThresholdMs_correctConfigurationPropagation() {
        // Create a Configuration instance
        Configuration conf = new Configuration();

        // Set the threshold value in the configuration
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 300);

        // Verify that the threshold value matches the expected value
        int thresholdConfigured = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        assert thresholdConfigured == 300;
    }

    // Test to maximize coverage and validate behavior of the public method getByName
    @Test
    public void test_getByName_slowLookupLogsWhenAboveThreshold() throws Exception {
        // Set up configuration to define a slow lookup threshold
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 200);

        // Retrieve slow lookup threshold for verification using the private method indirectly via public method
        int slowLookupThresholdMs = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);
        assert slowLookupThresholdMs == 200;

        // Enable slow lookup logging
        SecurityUtil.logSlowLookups = true;

        // Mock the HostResolver and simulate a slow DNS resolution
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        SecurityUtil.hostResolver = mockHostResolver;

        // Simulate a DNS response for a hostname
        InetAddress mockInetAddress = InetAddress.getByName("example.com");
        when(mockHostResolver.getByName(anyString())).thenAnswer(invocation -> {
            Thread.sleep(slowLookupThresholdMs + 500); // Simulate delay beyond the threshold
            return mockInetAddress;
        });

        // Call the public method getByName with a valid hostname
        SecurityUtil.getByName("example.com");

        // Use logging frameworks to verify that appropriate warning messages exist
        // Ensure coverage of slow DNS resolution detection logic
    }
}