package org.apache.hadoop.ha;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.HealthMonitor;
import org.junit.Test;

public class HealthMonitorConfigTest {

    @Test
    public void testCustomRpcTimeoutOverridesDefault() throws Exception {
        // 1. Create Configuration and set custom value
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY, 8000);

        // 2. Compute expected value dynamically
        long expectedRpcTimeout = conf.getInt(
            CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY,
            CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT);

        // 3. Mock external dependencies
        HAServiceTarget mockTarget = mock(HAServiceTarget.class);
        HAServiceProtocol mockProxy = mock(HAServiceProtocol.class);
        when(mockTarget.getHealthMonitorProxy(conf, (int) expectedRpcTimeout))
            .thenReturn(mockProxy);

        // 4. Invoke method under test
        HealthMonitor monitor = new HealthMonitor(conf, mockTarget);

        // Trigger connection via reflection to avoid daemon thread
        java.lang.reflect.Method tryConnect = HealthMonitor.class.getDeclaredMethod("tryConnect");
        tryConnect.setAccessible(true);
        tryConnect.invoke(monitor);

        // 5. Verify timeout propagated to RPC layer
        verify(mockTarget).getHealthMonitorProxy(conf, 8000);
    }
}