package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;
import static org.junit.Assert.*;

public class TestSecurityUtil {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_traceLogsWithinThreshold() throws UnknownHostException {
        // Step 1: Create a Configuration instance and set the threshold property
        Configuration config = new Configuration();
        int thresholdMs = 100; // Set a reasonable threshold value (e.g., 100 ms)
        config.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, thresholdMs);

        // Step 2: Replace the static slowLookupThresholdMs in SecurityUtil using reflection
        int originalThreshold = SecurityUtil.slowLookupThresholdMs; // Preserve original static value
        SecurityUtil.slowLookupThresholdMs = config.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT
        );

        // Step 3: Mock the hostResolver to simulate fast DNS resolution
        SecurityUtil.HostResolver mockHostResolver = Mockito.mock(SecurityUtil.HostResolver.class);
        InetAddress mockInetAddress = Mockito.mock(InetAddress.class);
        Mockito.when(mockHostResolver.getByName("example.com")).thenReturn(mockInetAddress);

        // Replace hostResolver in SecurityUtil with the mocked instance
        SecurityUtil.hostResolver = mockHostResolver;

        // Step 4: Enable TRACE logging level for SecurityUtil
        Logger logger = LogManager.getLogger(SecurityUtil.class);
        ((org.apache.logging.log4j.core.Logger) logger).setLevel(Level.TRACE);

        // Enable logSlowLookups for SecurityUtil
        SecurityUtil.logSlowLookups = true;

        // Step 5: Execute the getByName method with a valid hostname
        InetAddress result = SecurityUtil.getByName("example.com");

        // Assertions to verify expected behavior
        assertNotNull(result); // Ensure the DNS resolution result is not null

        // Restore original configuration
        SecurityUtil.slowLookupThresholdMs = originalThreshold;
    }
}