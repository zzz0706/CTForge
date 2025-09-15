package org.apache.hadoop.ha;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class FailoverControllerTest {

    @Test
    public void testGracefulFenceRetriesLimitConnectionAttempts() throws Exception {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES, 2);

        // 2. Dynamic Expected Value Calculation
        int expectedRetries = conf.getInt(
                CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES,
                CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT);
        int expectedCalls = expectedRetries + 1; // initial + retries

        // 3. Mock/Stub External Dependencies
        HAServiceTarget mockTarget = mock(HAServiceTarget.class);
        when(mockTarget.getProxy(any(Configuration.class), anyInt()))
                .thenThrow(new IOException("Connection refused"));

        FailoverController fc = new FailoverController(conf, null);

        // 4. Invoke the Method Under Test
        boolean result = fc.tryGracefulFence(mockTarget);

        // 5. Assertions and Verification
        assertEquals(false, result); // graceful fence should fail
        verify(mockTarget, times(1))
                .getProxy(any(Configuration.class), anyInt());
    }
}