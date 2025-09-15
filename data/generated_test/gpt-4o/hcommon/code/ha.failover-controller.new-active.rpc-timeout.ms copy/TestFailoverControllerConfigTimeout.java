package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test for verifying preFailoverChecks uses configuration-propagated timeout.
 */
public class TestFailoverControllerConfigTimeout {

    /**
     * Ensure that preFailoverChecks uses the correct timeout, taken from configuration,
     * when accessing a service via getProxy().
     */
    @Test
    public void testFailoverControllerPreFailoverChecksUsesConfiguredTimeout() throws Exception {
        // 1. Obtain the configuration and its value via API
        Configuration conf = new Configuration();
        int testTimeout = conf.getInt(
            CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY,
            CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT);

        // 2. Prepare the HAServiceTarget mock to validate getProxy() usage
        HAServiceTarget fromTarget = mock(HAServiceTarget.class);
        HAServiceTarget toTarget = mock(HAServiceTarget.class);
        HAServiceProtocol toProtocol = mock(HAServiceProtocol.class);
        HAServiceStatus standbyStatus = mock(HAServiceStatus.class);

        // Simulate addresses so failover check passes address validation
        when(fromTarget.getAddress()).thenReturn(new InetSocketAddress("service-1", 9000));
        when(toTarget.getAddress()).thenReturn(new InetSocketAddress("service-2", 9000));

        // When getProxy is called, return our protocol mock (capture timeout argument)
        ArgumentCaptor<Integer> timeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        when(toTarget.getProxy(eq(conf), timeoutCaptor.capture())).thenReturn(toProtocol);

        // The service protocol needs to return STANDBY and ready-to-be-active
        when(toProtocol.getServiceStatus()).thenReturn(standbyStatus);
        when(standbyStatus.getState()).thenReturn(HAServiceState.STANDBY);
        when(standbyStatus.isReadyToBecomeActive()).thenReturn(true);

        // 3. Prepare source requirement for the controller ctor (should be RequestSource instance, not null or Object)
        // HAServiceProtocol.RequestSource is an enum inside HAServiceProtocol in Hadoop 2.x
        HAServiceProtocol.RequestSource requestSource = HAServiceProtocol.RequestSource.REQUEST_BY_USER;

        // Instantiate the controller
        FailoverController controller = new FailoverController(conf, requestSource);

        // Invoke preFailoverChecks via reflection, as it's private
        java.lang.reflect.Method method = FailoverController.class.getDeclaredMethod(
            "preFailoverChecks", HAServiceTarget.class, HAServiceTarget.class, boolean.class);
        method.setAccessible(true);
        method.invoke(controller, fromTarget, toTarget, false);

        // 4. Verify that getProxy was invoked with the correct (configured) timeout
        verify(toTarget, atLeastOnce()).getProxy(eq(conf), anyInt());
        assertEquals("Timeout passed to getProxy should match configuration value",
            testTimeout, (int) timeoutCaptor.getValue());

        // Confirming correct HAServiceStatus state was checked
        verify(toProtocol, atLeastOnce()).getServiceStatus();
        verify(standbyStatus, atLeastOnce()).getState();
        verify(standbyStatus, atLeastOnce()).isReadyToBecomeActive();
    }
}