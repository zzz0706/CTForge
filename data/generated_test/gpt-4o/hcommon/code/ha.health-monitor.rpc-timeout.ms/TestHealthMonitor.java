package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HealthMonitor;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class TestHealthMonitor {
    @Test
    public void test_createProxy_withValidConfiguration() throws IOException {
        // Step 1: Use API to get configuration value
        Configuration conf = new Configuration();
        String rpcTimeoutKey = "ha.health-monitor.rpc-timeout.ms";
        int rpcTimeoutDefault = 45000;
        conf.setInt(rpcTimeoutKey, rpcTimeoutDefault);

        // Step 2: Prepare test conditions
        HAServiceTarget mockTarget = mock(HAServiceTarget.class);
        HAServiceProtocol mockProxy = mock(HAServiceProtocol.class);

        // Ensure correct stubbing with thenReturn()
        when(mockTarget.getHealthMonitorProxy(eq(conf), anyInt())).thenReturn(mockProxy);

        // Step 3: Test code
        HealthMonitor monitor = new HealthMonitor(conf, mockTarget);
        HAServiceProtocol proxy = monitor.createProxy();

        // Step 4: Post-test code
        assertNotNull(proxy);

        // Verify the mock method interaction with the configuration
        verify(mockTarget).getHealthMonitorProxy(conf, conf.getInt(rpcTimeoutKey, rpcTimeoutDefault));
    }
}