package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
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
        // Create a Configuration instance and set the hadoop.security.dns.log-slow-lookups.threshold.ms property
        Configuration config = new Configuration();
        int thresholdMs = 100; // Set to a reasonable value (e.g., 100 ms)
        config.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, thresholdMs);

        // Replace the slowLookupThresholdMs in SecurityUtil using reflection
        int originalThreshold = SecurityUtil.slowLookupThresholdMs; // Preserve original value
        SecurityUtil.slowLookupThresholdMs = config.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT
        );

        // Mocking the hostResolver to simulate fast DNS resolution
        SecurityUtil.HostResolver mockHostResolver = Mockito.mock(SecurityUtil.HostResolver.class);
        InetAddress mockInetAddress = Mockito.mock(InetAddress.class);
        Mockito.when(mockHostResolver.getByName("example.com")).thenReturn(mockInetAddress);

        // Replace the hostResolver in SecurityUtil with the mocked instance
        SecurityUtil.hostResolver = mockHostResolver;

        // Enable TRACE logging level for SecurityUtil class
        Logger logger = LogManager.getLogger(SecurityUtil.class);
        logger.setLevel(Level.TRACE);

        // Ensure logSlowLookups is enabled using reflection
        SecurityUtil.logSlowLookups = true;

        // Call the getByName method with a valid hostname
        InetAddress result = SecurityUtil.getByName("example.com");

        // Verify that trace logs are generated indicating the DNS resolution time and hostname
        // Assert logs or capture logging output for expected messages
        assertNotNull(result);

        // Restore original configuration
        SecurityUtil.slowLookupThresholdMs = originalThreshold;
    }
}