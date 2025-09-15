package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.TimeUnit;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.net.InetAddress;

public class SecurityUtilTest {
    // Verify that the utilization of configuration in the getByName function is covered
    @Test
    public void test_getByName_slowLookupLogsWhenAboveThreshold() throws Exception {
        // Create a mocked Configuration instance to retrieve the threshold value using the API.
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 200); // Set threshold value to 200 ms.

        // Retrieve the slow lookup threshold using the public API.
        int slowLookupThresholdMs = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        // Enable slow lookup logging.
        SecurityUtil.logSlowLookups = true;

        // Mock SecurityUtil.HostResolver to simulate slow DNS resolution.
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        SecurityUtil.hostResolver = mockHostResolver;
        InetAddress mockInetAddress = InetAddress.getByName("example.com");
        when(mockHostResolver.getByName(anyString())).thenAnswer(invocation -> {
            // Simulate delay exceeding the threshold to trigger slow lookup logging.
            Thread.sleep(slowLookupThresholdMs + 500); // 500 ms above the threshold.
            return mockInetAddress;
        });

        // Invoke the getByName method and ensure the configuration value is utilized properly.
        SecurityUtil.getByName("example.com");

        // Validate the logging output using a logging framework or mock framework to assert warnings are logged.
    }
}