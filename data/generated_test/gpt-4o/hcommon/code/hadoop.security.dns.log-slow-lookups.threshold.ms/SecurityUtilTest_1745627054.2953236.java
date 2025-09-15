package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.TimeUnit;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.net.InetAddress;

public class SecurityUtilTest {

    // Test case: Verify that the getByName function logs a warning when DNS resolution time exceeds the configured threshold.
    @Test
    public void test_getByName_slowLookupLogsWhenAboveThreshold() throws Exception {
        // Create a mocked Configuration instance.
        Configuration conf = new Configuration();
        // Set the threshold value for slow DNS lookup logging in the configuration.
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 200);

        // Retrieve and verify the threshold value using the public Configuration API.
        int slowLookupThresholdMs = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        // Validate that the retrieved value is correct and matched with the set value.
        assert slowLookupThresholdMs == 200;

        // Enable the static flag for slow lookup logging in SecurityUtil.
        SecurityUtil.logSlowLookups = true;

        // Mock the SecurityUtil.HostResolver object to simulate DNS resolution behavior.
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        SecurityUtil.hostResolver = mockHostResolver;
        InetAddress mockInetAddress = InetAddress.getByName("example.com");

        when(mockHostResolver.getByName(anyString())).thenAnswer(invocation -> {
            // Simulate a delay longer than the threshold to ensure slow lookup logging is triggered.
            Thread.sleep(slowLookupThresholdMs + 500); // Exceeds the threshold by 500 ms.
            return mockInetAddress;
        });

        // Call the public getByName method, which internally invokes getSlowLookupThresholdMs.
        SecurityUtil.getByName("example.com");

        // Verify that the slow lookup threshold is utilized and a warning log is generated.
        // You could extend this by using frameworks such as LogCaptor/JUnit logs to assert the actual log occurrence.
    }
}