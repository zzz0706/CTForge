package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.mockito.Mockito;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

public class SecurityUtilTest {
    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getByName_throwUnknownHostExceptionForInvalidHostname() {
        // Mock the hostResolver to simulate DNS resolution failure
        SecurityUtil.HostResolver mockHostResolver = mock(SecurityUtil.HostResolver.class);
        Mockito.when(mockHostResolver.getByName(Mockito.anyString()))
               .thenThrow(new UnknownHostException("Host not found"));

        // Call the getByName method with an invalid hostname
        UnknownHostException exception = assertThrows(
            UnknownHostException.class,
            () -> SecurityUtil.getByName("invalid.hostname")
        );

        // Assert that the exception contains the correct message
        assert exception.getMessage().contains("Host not found");
    }
}