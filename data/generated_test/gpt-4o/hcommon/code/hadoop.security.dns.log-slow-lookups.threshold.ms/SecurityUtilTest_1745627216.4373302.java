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
        // Create a Configuration instance and set hadoop.security.dns.log-slow-lookups.threshold.ms property
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 50);

        try (MockedStatic<SecurityUtil> mockedSecurityUtil = Mockito.mockStatic(SecurityUtil.class)) {
            // Mock logSlowLookups flag as disabled
            mockedSecurityUtil.when(() -> SecurityUtil.logSlowLookups).thenReturn(false);

            // Mock DNS resolution: simulate the hostResolver.getByName behavior
            InetAddress mockInetAddress = InetAddress.getByName("example.com");
            try (MockedStatic<NetUtils> mockedNetUtils = Mockito.mockStatic(NetUtils.class)) {
                mockedNetUtils.when(() -> NetUtils.fastInetAddrResolution("example.com"))
                        .thenReturn(mockInetAddress);

                // Call getByName method with mock behavior
                InetAddress result = SecurityUtil.getByName("example.com");

                // Assert that no log warnings or traces are generated
                mockedSecurityUtil.verify(() -> SecurityUtil.logSlowLookups, Mockito.times(1));
                Mockito.verify(mockedNetUtils, Mockito.times(1))
                        .when(() -> NetUtils.fastInetAddrResolution("example.com"));
            }
        }
    }
}