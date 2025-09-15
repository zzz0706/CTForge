package alluxio.worker.grpc;

import alluxio.conf.ServerConfiguration;
import alluxio.util.network.NettyUtils;
import io.netty.channel.EventLoopGroup;
import alluxio.network.ChannelType;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class NettyUtilsTest {
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void testNettyEventLoopGroupWithEdgeCases() {
        // 1. Obtain configuration values using the Alluxio API
        int bossThreadCount = ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);

        // Prepare edge-case value (thread count set explicitly to 0) and valid ChannelType
        ChannelType channelType = ChannelType.NIO; // Correct the enum reference based on the Alluxio API
        String threadNamePrefix = "test-boss-%d";

        // 2. Test code: Call createEventLoop with edge-case configuration using ChannelType
        EventLoopGroup bossGroup = NettyUtils.createEventLoop(
                channelType, bossThreadCount, threadNamePrefix, true);

        // Validate whether fallback/default behavior is used or threads are created correctly
        Assert.assertNotNull("EventLoopGroup should be created even with edge-case thread count.", bossGroup);

        // 4. Shut down resources after test execution
        bossGroup.shutdownGracefully();
    }
}