package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.FailoverController;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.*;

public class FailoverControllerTest {

    @Test
    public void testPreFailoverChecksRpcTimeoutPropagation() throws Exception {
        // Step 1: Prepare mock objects and inputs
        Configuration conf = new Configuration();

        // Ensure that configuration values are accessed via API, not directly set
        int rpcTimeoutToNewActive = FailoverController.getRpcTimeoutToNewActive(conf);

        HAServiceTarget mockFromSvc = Mockito.mock(HAServiceTarget.class);
        HAServiceTarget mockTargetSvc = Mockito.mock(HAServiceTarget.class);
        HAServiceProtocol mockProtocol = Mockito.mock(HAServiceProtocol.class);

        // Mock responses for HAServiceTarget-related operations
        InetSocketAddress mockFromAddress = new InetSocketAddress("localhost", 8020);
        InetSocketAddress mockTargetAddress = new InetSocketAddress("localhost", 8021);

        when(mockFromSvc.getAddress()).thenReturn(mockFromAddress);
        when(mockTargetSvc.getAddress()).thenReturn(mockTargetAddress);

        // Mock the return for proxy creation with the rpcTimeoutToNewActive
        when(mockTargetSvc.getProxy(conf, rpcTimeoutToNewActive)).thenReturn(mockProtocol);

        // Mock other test-relevant service protocol behavior
        HAServiceStatus mockStatus = Mockito.mock(HAServiceStatus.class);
        when(mockProtocol.getServiceStatus()).thenReturn(mockStatus);
        when(mockStatus.getState()).thenReturn(HAServiceState.STANDBY); // Use the correct HAServiceState
        when(mockStatus.isReadyToBecomeActive()).thenReturn(true);

        // Step 2: Instantiate FailoverController
        FailoverController failoverController = new FailoverController(conf, null);

        // Step 3: Test private method using reflection
        java.lang.reflect.Method method = FailoverController.class.getDeclaredMethod(
            "preFailoverChecks", HAServiceTarget.class, HAServiceTarget.class, boolean.class
        );
        method.setAccessible(true);
        method.invoke(failoverController, mockFromSvc, mockTargetSvc, false);

        // Step 4: Verify that rpcTimeoutToNewActive was propagated and utilized
        verify(mockTargetSvc).getProxy(conf, rpcTimeoutToNewActive);
        verify(mockProtocol).getServiceStatus();

        // Assert that no exceptions were thrown
        // This implies the logic executed correctly during the test process
    }
}