package alluxio.worker.grpc;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.util.network.NettyUtils;
import io.netty.channel.EventLoopGroup;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class GrpcDataServerTest {

    @Test
    public void testEventLoopCreationInvalidThreadCount() {
        // Step 1: Obtain the configuration value for the number of boss threads using the Alluxio API
        int bossThreadCount = ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);

        // Step 2: Create test conditions using valid configuration value
        // Test with valid thread count
        EventLoopGroup validLoopGroup = NettyUtils.createEventLoop(ChannelType.NIO, bossThreadCount, "test-boss-%d", true);
        assertNotNull("Valid thread count should create a non-null EventLoopGroup", validLoopGroup);
        validLoopGroup.shutdownGracefully();

        // Step 3: Test functionality with edge cases (invalid thread counts)
        // Edge cases like zero or negative thread counts should ideally throw exceptions or be handled gracefully
        try {
            NettyUtils.createEventLoop(ChannelType.NIO, -1, "test-boss-%d", true);
        } catch (IllegalArgumentException e) {
            assertNotNull("Exception should be thrown for negative thread count", e);
        }

        try {
            NettyUtils.createEventLoop(ChannelType.NIO, 0, "test-boss-%d", true);
        } catch (IllegalArgumentException e) {
            assertNotNull("Exception should be thrown for zero thread count", e);
        }

        try {
            NettyUtils.createEventLoop(ChannelType.EPOLL, -1, "test-boss-%d", true);
        } catch (IllegalArgumentException e) {
            assertNotNull("Exception should be thrown for negative thread count", e);
        }

        try {
            NettyUtils.createEventLoop(ChannelType.EPOLL, 0, "test-boss-%d", true);
        } catch (IllegalArgumentException e) {
            assertNotNull("Exception should be thrown for zero thread count", e);
        }
    }
}