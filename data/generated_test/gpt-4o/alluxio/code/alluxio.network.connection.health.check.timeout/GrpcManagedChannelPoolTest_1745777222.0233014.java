package alluxio.grpc;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

public class GrpcManagedChannelPoolTest {

    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void testWaitForChannelReady_readyScenario() throws Exception {
        // 1. Use the configuration API to obtain the configuration value for the health check timeout.
        InstancedConfiguration configuration = InstancedConfiguration.defaults();
        long healthCheckTimeoutMs = configuration.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

        // 2. Mock a ManagedChannel to mimic an instant READY state.
        ManagedChannel mockChannel = Mockito.mock(ManagedChannel.class);
        Mockito.when(mockChannel.getState(Mockito.anyBoolean())).thenReturn(ConnectivityState.READY);

        // 3. Use reflection to test the private method indirectly.
        GrpcManagedChannelPool channelPool = new GrpcManagedChannelPool();

        // Reflect to access the `waitForChannelReady` method since it is private.
        java.lang.reflect.Method method = GrpcManagedChannelPool.class.getDeclaredMethod("waitForChannelReady", ManagedChannel.class, long.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(channelPool, mockChannel, healthCheckTimeoutMs);

        // 4. Validate that the result is correct (READY state is detected).
        Assert.assertTrue("The channel should correctly detect the READY state within the timeout.", result);
    }
}