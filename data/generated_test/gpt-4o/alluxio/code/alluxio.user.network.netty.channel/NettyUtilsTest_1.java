package alluxio.util.network;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NettyUtils;
import alluxio.network.ChannelType;
import org.junit.Assert;
import org.junit.Test;

public class NettyUtilsTest {
    /**
     * Test case: testGetUserChannel_Epoll
     * Objective: Verify that the function retrieves the ChannelType EPOLL when the configuration is set to EPOLL.
     * Prerequisites: A valid AlluxioConfiguration instance with USER_NETWORK_NETTY_CHANNEL set to EPOLL.
     */
    @Test
    public void testGetUserChannel_Epoll() {
        // 1. Prepare the test conditions: Create an InstancedConfiguration instance and set the property USER_NETWORK_NETTY_CHANNEL to EPOLL
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        conf.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, ChannelType.EPOLL.name());

        // 2. Invoke NettyUtils.getUserChannel with the configuration
        ChannelType channelType = NettyUtils.getUserChannel(conf);

        // 3. Assert that the returned ChannelType matches EPOLL
        Assert.assertEquals(ChannelType.EPOLL, channelType);
    }
}