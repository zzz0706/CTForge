package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StopWatch;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;

public class SecurityUtilTest {

    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_doNotLogWhenLogSlowLookupsDisabled() throws Exception {
        // Set up the configuration with a valid threshold value
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 100);

        // Mocked InetAddress to simulate a DNS resolution
        InetAddress mockInetAddress = InetAddress.getByName("example.com");

        try (MockedStatic<NetUtils> mockedNetUtils = Mockito.mockStatic(NetUtils.class);
             MockedStatic<SecurityUtil> mockedSecurityUtil = Mockito.mockStatic(SecurityUtil.class);
             MockedStatic<StopWatch> mockedStopWatch = Mockito.mockStatic(StopWatch.class)) {

            // Mock SecurityUtil static behavior
            mockedSecurityUtil.when(() -> SecurityUtil.logSlowLookups).thenReturn(false);

            // Simulate NetUtils.fastInetAddrResolution behavior
            mockedNetUtils.when(() -> NetUtils.fastInetAddrResolution("example.com"))
                    .thenReturn(mockInetAddress);

            // Call the method being tested
            InetAddress resolvedAddress = SecurityUtil.getByName("example.com");

            // Assertions
            assertEquals(mockInetAddress, resolvedAddress);

            // Verify no logging or stopwatch interactions occur when logSlowLookups is disabled
            verifyNoInteractions(mockedStopWatch);
        }
    }
}