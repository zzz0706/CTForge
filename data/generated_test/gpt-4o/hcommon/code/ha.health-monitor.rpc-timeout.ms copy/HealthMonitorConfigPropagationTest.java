package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.HealthMonitor;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class HealthMonitorConfigPropagationTest {
    /**
     * Test that HealthMonitor.createProxy() propagates the value of
     * ha.health-monitor.rpc-timeout.ms from the Configuration to the target's
     * getHealthMonitorProxy() method under various functional workloads.
     */
    @Test
    public void testCreateProxy_UsesConfiguredRpcTimeout() throws Exception {

        final Configuration conf = new Configuration();
        final int configuredTimeout = conf.getInt(
                CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT);

  
        // Create a mock HAServiceTarget
        HAServiceTarget mockTarget = mock(HAServiceTarget.class);
        HAServiceProtocol dummyProxy = mock(HAServiceProtocol.class);

        // Setup behavior of mockTarget to return dummyProxy when called with any Configuration and any int
        when(mockTarget.getHealthMonitorProxy(any(Configuration.class), anyInt()))
                .thenReturn(dummyProxy);

        // Construct HealthMonitor with mocked target and API-driven config value
        HealthMonitor monitor = new HealthMonitor(conf, mockTarget);

        // 3.  Should internally call getHealthMonitorProxy with 'configuredTimeout'
        monitor.createProxy();

        // Verify that getHealthMonitorProxy was called with the correct timeout from the API
        ArgumentCaptor<Integer> timeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(mockTarget).getHealthMonitorProxy(eq(conf), timeoutCaptor.capture());

        int actualTimeoutUsed = timeoutCaptor.getValue();
        assertEquals("HealthMonitor should propagate configuration timeout to target",
                configuredTimeout, actualTimeoutUsed);
    }
}