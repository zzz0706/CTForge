package alluxio.util.network;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NettyUtilsTest {
    
    @Test
    public void test_getClientChannelClass_with_epoll_and_domain_socket() {
        // Prepare the test conditions: Initialize AlluxioProperties
        AlluxioProperties properties = new AlluxioProperties();
        
        // Set the required configuration property to simulate testing conditions
        properties.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "EPOLL");

        // Initialize InstancedConfiguration with AlluxioProperties
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // Test code: Call getClientChannelClass with the domain socket flag
        boolean isDomainSocket = true; // domain socket enabled
        Class<? extends Channel> channelClass = NettyUtils.getClientChannelClass(isDomainSocket, conf);

        // Assert the expected result: Check that the proper Channel class is returned
        assertEquals(EpollDomainSocketChannel.class, channelClass);
    }
}