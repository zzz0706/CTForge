package alluxio.grpc;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

public class GrpcManagedChannelPoolTest {
    @Test
    public void testWaitForChannelReady_timeoutScenario() throws Exception {
        // Obtain configuration value using Alluxio API
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        long healthCheckTimeoutMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

        // Mock a ManagedChannel that always remains in the CONNECTING state
        ManagedChannel mockChannel = mock(ManagedChannel.class);
        when(mockChannel.getState(true)).thenAnswer(invocation -> ConnectivityState.CONNECTING);

        // Create a subclass to test the method with a workaround for private access
        GrpcManagedChannelPool pool = new GrpcManagedChannelPool();
        
        // Create a utility method to invoke the private method using reflection
        java.lang.reflect.Method method = GrpcManagedChannelPool.class.getDeclaredMethod("waitForChannelReady", ManagedChannel.class, long.class);
        method.setAccessible(true);

        // Test: Call waitForChannelReady with the mocked ManagedChannel using reflection
        boolean result = (boolean) method.invoke(pool, mockChannel, healthCheckTimeoutMs);

        // Assertion: Ensure waitForChannelReady returns false after timeout
        assertFalse("Expected waitForChannelReady to return false after exceeding the timeout", result);

        // Optional: Verify any interactions with the mock, if needed
        verify(mockChannel, atLeastOnce()).getState(true);
    }
}