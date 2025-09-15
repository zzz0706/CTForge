package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StopWatch;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.junit.Test;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.net.InetAddress;

public class SecurityUtilTest {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_doNotLogWhenLogSlowLookupsDisabled() throws Exception {
        // Create a Configuration instance and set hadoop.security.dns.log-slow-lookups.threshold.ms property
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 50);

        // Mocked behavior for InetAddress resolution
        InetAddress mockInetAddress = InetAddress.getByName("example.com");

        try (MockedStatic<NetUtils> mockedNetUtils = Mockito.mockStatic(NetUtils.class);
             MockedStatic<SecurityUtil> mockedSecurityUtil = Mockito.mockStatic(SecurityUtil.class);
             MockedStatic<StopWatch> mockedStopWatch = Mockito.mockStatic(StopWatch.class)) {

            // Simulate SecurityUtil settings for log slow lookups
            mockedSecurityUtil.when(() -> SecurityUtil.logSlowLookups).thenReturn(false);

            // Simulate behavior of NetUtils.fastInetAddrResolution
            mockedNetUtils.when(() -> NetUtils.fastInetAddrResolution("example.com"))
                    .thenReturn(mockInetAddress);

            // Call SecurityUtil.getByName
            InetAddress result = SecurityUtil.getByName("example.com");
            
            // Validate the correct address was resolved
            assert result.equals(mockInetAddress);
            
            // Validate no logging or timing behavior occurred
            mockedStopWatch.verifyNoInteractions();
        }
    }
}