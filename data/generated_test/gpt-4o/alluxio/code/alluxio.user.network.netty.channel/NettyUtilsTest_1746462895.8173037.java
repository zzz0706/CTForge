package alluxio.util.network;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Assert;
import org.junit.Test;

public class NettyUtilsTest {

    @Test
    public void testGetClientChannelClass_NIO() {
        // Prepare the test conditions
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "NIO");
        AlluxioConfiguration conf = new InstancedConfiguration(properties);
        
        // Test code
        Class<?> channelClass = NettyUtils.getClientChannelClass(false, conf);

        // Verify the test result
        Assert.assertEquals(NioSocketChannel.class, channelClass);
    }
}