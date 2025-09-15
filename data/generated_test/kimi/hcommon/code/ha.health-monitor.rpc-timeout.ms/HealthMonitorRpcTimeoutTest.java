package org.apache.hadoop.ha;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Thread.class, HealthMonitor.class})
public class HealthMonitorRpcTimeoutTest {

    @Test
    public void testRpcTimeoutPropagatedOnEachReconnect() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();
        // Override in test resources if necessary; otherwise rely on default
        // conf.setInt(CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY, 12000);

        // 2. Dynamic expected value calculation
        int expectedRpcTimeout = conf.getInt(
                CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT);

        // 3. Mock external dependencies
        HAServiceTarget mockTarget = mock(HAServiceTarget.class);
        // First call throws IOException, second succeeds
        when(mockTarget.getHealthMonitorProxy(eq(conf), anyInt()))
                .thenThrow(new IOException("simulated failure"))
                .thenReturn(mock(HAServiceProtocol.class));

        // Stub Thread.sleep to avoid delays
        PowerMockito.mockStatic(Thread.class);
        PowerMockito.doNothing().when(Thread.class);
        Thread.sleep(anyLong());

        // 4. Invoke the method under test
        HealthMonitor monitor = new HealthMonitor(conf, mockTarget);
        monitor.start();

        // Wait for two proxy creation attempts
        // HealthMonitor retries after Thread.sleep(connectRetryInterval)
        // Since we stubbed sleep, the loop will run quickly
        Thread.sleep(100); // brief wait for background thread to finish

        monitor.shutdown();
        monitor.join();

        // 5. Assertions and verification
        verify(mockTarget, times(2))
                .getHealthMonitorProxy(eq(conf), eq(expectedRpcTimeout));
    }
}