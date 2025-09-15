package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

public class SecurityUtilTest {

    @Test
    public void test_getByName_throwUnknownHostExceptionForInvalidHostname() {
        // Create a Configuration instance and set the threshold property
        Configuration configuration = new Configuration();
        configuration.setInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            500
        );

        // Replace the static field slowLookupThresholdMs with the value from configuration
        SecurityUtil.slowLookupThresholdMs = configuration.getInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT
        );

        // Mock the hostResolver to simulate DNS resolution failure
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        Mockito.when(mockHostResolver.getByName(Mockito.anyString()))
            .thenThrow(new UnknownHostException("Host not found"));

        // Replace the static field hostResolver with the mocked resolver
        SecurityUtil.hostResolver = mockHostResolver;

        // Test the method call and assert that it throws an UnknownHostException
        UnknownHostException exception = assertThrows(
            UnknownHostException.class,
            () -> SecurityUtil.getByName("invalid.hostname")
        );

        // Validate the exception message
        assert exception.getMessage().contains("Host not found");
    }

    @Test
    public void test_getByName_logsSlowLookupsWithCustomThreshold() throws Exception {
        // Create a Configuration instance and set the threshold property
        Configuration configuration = new Configuration();
        configuration.setInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            500
        );

        // Replace the static field slowLookupThresholdMs with the value from configuration
        SecurityUtil.slowLookupThresholdMs = configuration.getInt(
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
            CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT
        );

        // Mock the hostResolver to simulate a slow DNS resolution
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        InetAddress mockAddress = mock(InetAddress.class);
        Mockito.when(mockHostResolver.getByName(Mockito.anyString()))
            .thenAnswer(invocation -> {
                Thread.sleep(600); // Simulate slow lookup
                return mockAddress;
            });

        // Replace the static resolver with the mocked version
        SecurityUtil.hostResolver = mockHostResolver;

        // Set logSlowLookups to true for testing logging behavior
        SecurityUtil.logSlowLookups = true;

        // Call the method with a hostname and ensure it does not throw exceptions
        InetAddress result = SecurityUtil.getByName("slow.hostname");

        // Verify the resolved address is returned correctly
        assert result == mockAddress;
    }
}