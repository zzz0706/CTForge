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
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY, 12000);

        // 2. Prepare the test conditions
        HAServiceTarget mockTarget = mock(HAServiceTarget.class);
        // First call throws IOException, second succeeds
        when(mockTarget.getHealthMonitorProxy(eq(conf), anyInt()))
                .thenThrow(new IOException("simulated failure"))
                .thenReturn(mock(HAServiceProtocol.class));

        // Stub Thread.sleep to avoid delays
        PowerMockito.mockStatic(Thread.class);
        PowerMockito.doNothing().when(Thread.class);
        Thread.sleep(anyLong());

        // 3. Test code
        HealthMonitor monitor = new HealthMonitor(conf, mockTarget);
        monitor.start();

        // Allow background thread to retry
        Thread.sleep(100);

        monitor.shutdown();
        monitor.join();

        // 4. Code after testing
        int expectedRpcTimeout = conf.getInt(
                CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT);
        verify(mockTarget, times(2))
                .getHealthMonitorProxy(eq(conf), eq(expectedRpcTimeout));
    }
}