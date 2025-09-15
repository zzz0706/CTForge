package org.apache.hadoop.ha;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.HealthMonitor;
import org.junit.Test;

import java.io.IOException;

public class HealthMonitorConfigTest {

    @Test
    public void testDefaultRpcTimeoutIsUsedWhenNoOverride() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();

        // 2. Dynamic expected value calculation
        long expectedRpcTimeout = conf.getInt(
            CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY,
            CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT);

        // 3. Mock/stub external dependencies
        HAServiceTarget mockTarget = mock(HAServiceTarget.class);
        HAServiceProtocol mockProxy = mock(HAServiceProtocol.class);
        when(mockTarget.getHealthMonitorProxy(conf, (int) expectedRpcTimeout))
            .thenReturn(mockProxy);

        // 4. Invoke the method under test
        HealthMonitor monitor = new HealthMonitor(conf, mockTarget);
        // Trigger connection by starting the MonitorDaemon thread
        monitor.start();
        // Wait a bit so the thread reaches getHealthMonitorProxy
        Thread.sleep(100);
        monitor.shutdown();

        // 5. Assertions and verification
        verify(mockTarget, atLeastOnce())
            .getHealthMonitorProxy(conf, (int) expectedRpcTimeout);
        assertEquals(expectedRpcTimeout, 45000);
    }
}