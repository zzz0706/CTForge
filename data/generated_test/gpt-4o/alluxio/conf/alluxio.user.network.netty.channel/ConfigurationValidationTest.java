package alluxio.conf;

import alluxio.util.network.NettyUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;
import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class ConfigurationValidationTest {
    @Test
    public void testUserNetworkNettyChannelConfiguration() {
        // Step 1: Create an AlluxioConfiguration instance using InstancedConfiguration
        AlluxioConfiguration conf = InstancedConfiguration.defaults();

        // Step 2: Obtain the USER_NETWORK_NETTY_CHANNEL property value
        String nettyChannelConfig = conf.get(PropertyKey.USER_NETWORK_NETTY_CHANNEL);

        // Step 3: Validate the retrieved configuration value
        Assert.assertNotNull("Configuration for 'alluxio.user.network.netty.channel' must not be null", nettyChannelConfig);
        Assert.assertFalse("Configuration for 'alluxio.user.network.netty.channel' must not be empty", nettyChannelConfig.trim().isEmpty());

        // Validate if the configuration value matches either "EPOLL" or "NIO"
        boolean isValid = "EPOLL".equalsIgnoreCase(nettyChannelConfig) || "NIO".equalsIgnoreCase(nettyChannelConfig);
        Assert.assertTrue("Invalid value for 'alluxio.user.network.netty.channel'. Allowed values are 'EPOLL' or 'NIO'", isValid);

        // Step 4: Check if the client channel class matches the expected type based on the configuration
        if ("EPOLL".equalsIgnoreCase(nettyChannelConfig)) {
            Class<? extends Channel> channelClass = NettyUtils.getClientChannelClass(true, conf);
            Assert.assertEquals("Domain socket should only use EPOLL channel type", EpollDomainSocketChannel.class, channelClass);
        } else if ("NIO".equalsIgnoreCase(nettyChannelConfig)) {
            Class<? extends Channel> channelClass = NettyUtils.getClientChannelClass(false, conf);
            Assert.assertEquals("NIO should return NioSocketChannel", NioSocketChannel.class, channelClass);
        }
    }
}