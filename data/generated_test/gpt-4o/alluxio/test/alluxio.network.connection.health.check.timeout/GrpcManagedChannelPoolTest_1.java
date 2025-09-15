package alluxio.grpc;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.Assert;
import org.junit.Test;

public class GrpcManagedChannelPoolTest {

    @Test
    public void test_waitForChannelReady_exceeds_health_check_timeout() throws Exception {
        // 1. Use the Alluxio 2.1.0 API to obtain configuration values correctly
        InstancedConfiguration configuration = InstancedConfiguration.defaults();
        configuration.set(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT, "2000ms");
        long healthCheckTimeoutMs = configuration.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

        // 2. Prepare the test conditions
        // Create a ManagedChannel instance in IDLE state
        ManagedChannel idleChannel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        // Create a GrpcManagedChannelPool instance (no arguments constructor used)
        GrpcManagedChannelPool channelPool = new GrpcManagedChannelPool();

        // 3. Test the waitForChannelReady method using reflection to handle private method access
        java.lang.reflect.Method waitForChannelReadyMethod = GrpcManagedChannelPool.class
                .getDeclaredMethod("waitForChannelReady", ManagedChannel.class, long.class);
        waitForChannelReadyMethod.setAccessible(true);

        boolean channelReadyStatus = (boolean) waitForChannelReadyMethod.invoke(channelPool, idleChannel, healthCheckTimeoutMs);

        // Assert the test result
        Assert.assertFalse("Channel should not be ready within the configured timeout.", channelReadyStatus);

        // 4. Clean up resources
        idleChannel.shutdown();
    }
}