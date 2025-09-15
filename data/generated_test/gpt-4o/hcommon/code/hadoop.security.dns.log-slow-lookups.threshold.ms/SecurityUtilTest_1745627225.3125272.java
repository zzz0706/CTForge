package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.net.NetUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.junit.Test;

import java.net.InetAddress;

public class SecurityUtilTest {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_doNotLogWhenLogSlowLookupsDisabled() throws Exception {
        // Create a Configuration instance and set hadoop.security.dns.log-slow-lookups.threshold.ms property
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 50);

        // Mock hosts resolver behavior
        InetAddress mockInetAddress = InetAddress.getByName("example.com");

        try (MockedStatic<NetUtils> mockedNetUtils = Mockito.mockStatic(NetUtils.class);
             MockedStatic<SecurityUtil> mockedSecurityUtil = Mockito.mockStatic(SecurityUtil.class)) {

            // Simulate SecurityUtil behavior
            mockedSecurityUtil.when(() -> SecurityUtil.logSlowLookups).thenReturn(false);

            mockedNetUtils.when(() -> NetUtils.fastInetAddrResolution("example.com"))
                    .thenReturn(mockInetAddress);

         }
}