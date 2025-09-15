package alluxio.util.network;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NettyUtilsTest {

    @Test
    public void test_getClientChannelClass_with_epoll_and_domain_socket() {
        // 1. Prepare the test conditions: Initialize AlluxioProperties and set configurations.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "EPOLL");

        // Initialize the AlluxioConfiguration object with the prepared properties.
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // 2. Test code: Call getClientChannelClass with domain socket enabled.
        boolean isDomainSocket = true; // domain socket enabled
        Class<? extends Channel> channelClass = NettyUtils.getClientChannelClass(isDomainSocket, conf);

        // 3. Assert the expected result: Check that the proper Channel class is returned.
        assertEquals(EpollDomainSocketChannel.class, channelClass);
    }

    @Test
    public void test_getClientChannelClass_with_epoll_without_domain_socket() {
        // 1. Prepare the test conditions: Initialize AlluxioProperties and set configurations.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "EPOLL");

        // Initialize the AlluxioConfiguration object with the prepared properties.
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // 2. Test code: Call getClientChannelClass with domain socket disabled.
        boolean isDomainSocket = false; // domain socket not enabled
        Class<? extends Channel> channelClass = NettyUtils.getClientChannelClass(isDomainSocket, conf);

        // 3. Assert the expected result: Check that the proper Channel class is returned.
        assertEquals(EpollSocketChannel.class, channelClass);
    }

    @Test
    public void test_getClientChannelClass_with_nio() {
        // 1. Prepare the test conditions: Initialize AlluxioProperties and set configurations.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "NIO");

        // Initialize the AlluxioConfiguration object with the prepared properties.
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // 2. Test code: Call getClientChannelClass with domain socket set to false to avoid exception.
        boolean isDomainSocket = false; // domain socket not applicable for NIO test
        Class<? extends Channel> channelClass = NettyUtils.getClientChannelClass(isDomainSocket, conf);

        // 3. Assert the expected result: Check that the proper Channel class is returned.
        assertEquals(NioSocketChannel.class, channelClass);
    }

    @Test(expected = RuntimeException.class)
    public void test_getClientChannelClass_with_invalid_channel_type() {
        // 1. Prepare the test conditions: Initialize AlluxioProperties and set invalid channel type.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "INVALID_CHANNEL_TYPE");

        // Initialize the AlluxioConfiguration object with the prepared properties.
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // 2. Test code: Call getClientChannelClass with domain socket set to false to handle invalid type.
        boolean isDomainSocket = false; // Arbitrary value since it should fail before considering this.
        NettyUtils.getClientChannelClass(isDomainSocket, conf);
    }
}