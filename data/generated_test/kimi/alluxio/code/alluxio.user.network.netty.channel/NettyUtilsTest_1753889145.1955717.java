package alluxio.util.network;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollDomainSocketChannel;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NettyUtilsTest {

  @Test
  public void EPOLL_domain_socket_returns_EpollDomainSocketChannel() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare the test conditions.
    conf.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "EPOLL");
    boolean isDomainSocket = true;

    // 3. Test code.
    Class<? extends Channel> actual = NettyUtils.getClientChannelClass(isDomainSocket, conf);

    // 4. Code after testing.
    Class<? extends Channel> expected = EpollDomainSocketChannel.class;
    assertEquals(expected, actual);
  }
}