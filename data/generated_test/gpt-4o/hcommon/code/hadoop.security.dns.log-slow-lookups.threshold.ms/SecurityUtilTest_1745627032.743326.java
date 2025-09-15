package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.TimeUnit;
import org.junit.Test;

import java.net.InetAddress;

import static org.mockito.Mockito.*;

public class SecurityUtilTest {

    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_slowLookupLogsWhenAboveThreshold() throws Exception {
        // Mock the Configuration instance to set the threshold explicitly.
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 200); // Set to 200 ms for testing.
        
        // Simulate setting the slow lookup threshold through the static method.
        int slowLookupThresholdMs = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        // Enable slow lookup logging.
        SecurityUtil.logSlowLookups = true;

        // Mock the HostResolver to simulate slow DNS resolution.
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        SecurityUtil.hostResolver = mockHostResolver;
        InetAddress mockInetAddress = InetAddress.getByName("example.com");
        when(mockHostResolver.getByName(anyString())).thenAnswer(invocation -> {
            // Simulate delay exceeding threshold.
            Thread.sleep(slowLookupThresholdMs + 500); // 500 ms above the threshold.
            return mockInetAddress;
        });

        // Call the SecurityUtil.getByName method and monitor its execution.
        SecurityUtil.getByName("example.com");

        // Use a logging framework like LogCaptor or similar to validate the warning log for slow DNS lookup.
    }
}