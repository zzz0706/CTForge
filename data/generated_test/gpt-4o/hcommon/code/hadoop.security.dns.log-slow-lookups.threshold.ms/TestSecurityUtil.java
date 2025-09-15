package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.mockito.Mockito;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestSecurityUtil {

    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_traceLogsWithinThreshold() throws UnknownHostException {
        // Mocking the hostResolver to simulate fast DNS resolution
        SecurityUtil.HostResolver mockHostResolver = Mockito.mock(SecurityUtil.HostResolver.class);
        InetAddress mockInetAddress = Mockito.mock(InetAddress.class);
        Mockito.when(mockHostResolver.getByName("example.com")).thenReturn(mockInetAddress);

        // Replace the hostResolver in SecurityUtil with the mocked instance
        SecurityUtil.hostResolver = mockHostResolver;

        // Ensure TRACE logging level is enabled for SecurityUtil class
        Logger logger = LogManager.getLogger(SecurityUtil.class);
        logger.setLevel(Level.TRACE);

        // Call the getByName method with a valid hostname
        InetAddress result = SecurityUtil.getByName("example.com");

        // Verify that trace logs are generated indicating the DNS resolution time and hostname
        // Mock logSlowLookups or leverage a log capturing tool to assert logged output
        // Expected trace log: "Name lookup for example.com took <time> ms."
    }
}