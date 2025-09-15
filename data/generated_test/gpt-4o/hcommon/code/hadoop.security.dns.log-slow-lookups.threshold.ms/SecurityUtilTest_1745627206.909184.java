package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.net.NetUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.junit.Test;

import java.net.InetAddress;

public class SecurityUtilTest {
    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_doNotLogWhenLogSlowLookupsDisabled() throws Exception {
        // Create a Configuration instance and fetch the hadoop.security.dns.log-slow-lookups.threshold.ms value
        Configuration conf = new Configuration();
        int slowLookupThresholdMs = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        // Disable logSlowLookups in the SecurityUtil class using Mockito to mock the static field
        try (MockedStatic<SecurityUtil> mockedSecurityUtil = Mockito.mockStatic(SecurityUtil.class)) {
            mockedSecurityUtil.when(() -> SecurityUtil.logSlowLookups).thenReturn(false);

            // Mock the hostResolver.getByName method to simulate DNS resolution
            InetAddress mockInetAddress = InetAddress.getByName("example.com");
            try (MockedStatic<NetUtils> mockedNetUtils = Mockito.mockStatic(NetUtils.class)) {
                mockedNetUtils.when(() -> NetUtils.fastInetAddrResolution("example.com"))
                        .thenReturn(mockInetAddress);

                // Call the SecurityUtil.getByName method with a valid hostname
                InetAddress result = SecurityUtil.getByName("example.com");

                // Assert correct behavior (verify no logs are generated)
                mockedSecurityUtil.verify(() -> SecurityUtil.logSlowLookups, Mockito.times(1));
            }
        }
    }
}