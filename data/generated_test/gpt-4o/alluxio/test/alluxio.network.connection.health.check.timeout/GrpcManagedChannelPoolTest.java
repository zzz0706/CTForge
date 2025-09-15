package alluxio.grpc;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcManagedChannelPool;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

/**
 * Unit test for GrpcManagedChannelPool.
 */
public class GrpcManagedChannelPoolTest {

    private GrpcManagedChannelPool mChannelPool;
    private AlluxioConfiguration mConfig;

    @Before
    public void setUp() {
        // Initialize GrpcManagedChannelPool and configuration using InstancedConfiguration
        mConfig = InstancedConfiguration.defaults();
        mChannelPool = new GrpcManagedChannelPool();
    }

    @Test
    public void testWaitForChannelReady_channelTransitionsToReady() throws Exception {
        // 1. Obtain the configuration value correctly using Alluxio 2.1.0 API.
        long healthCheckTimeoutMs = mConfig.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

        // 2. Prepare test conditions.
        // Create a mock ManagedChannel object.
        ManagedChannel mockChannel = mock(ManagedChannel.class);

        // Simulate the channel transitioning states: CONNECTING -> READY.
        when(mockChannel.getState(anyBoolean()))
                .thenReturn(ConnectivityState.CONNECTING) // Initial state
                .thenReturn(ConnectivityState.READY);     // Final successful state

        // 3. Test the functionality by invoking the private method.
        boolean result = invokeWaitForChannelReady(mockChannel, healthCheckTimeoutMs);

        // 4. Validate the test result.
        assert result : "Expected the method to return true when the channel transitions to READY within timeout.";
    }

    /**
     * Invokes the private `waitForChannelReady` method of GrpcManagedChannelPool using reflection.
     *
     * @param channel  the ManagedChannel to check
     * @param timeoutMs the timeout in milliseconds
     * @return true if the channel transitions to READY state, otherwise false
     * @throws Exception if reflection fails
     */
    private boolean invokeWaitForChannelReady(ManagedChannel channel, long timeoutMs) throws Exception {
        java.lang.reflect.Method method = GrpcManagedChannelPool.class.getDeclaredMethod(
                "waitForChannelReady", ManagedChannel.class, long.class);
        method.setAccessible(true);
        return (boolean) method.invoke(mChannelPool, channel, timeoutMs);
    }
}