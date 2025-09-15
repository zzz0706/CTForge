package alluxio.util.network;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollSocketChannel;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NettyUtilsTest {

  @Test
  public void EPOLL_available_returns_EpollSocketChannel() throws Exception {
    // 1. Create AlluxioConfiguration without overriding the key
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Force the system to believe epoll is available
    ((InstancedConfiguration) conf).set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "EPOLL");

    // 3. Invoke the method under test
    Class<? extends Channel> actual = NettyUtils.getClientChannelClass(false, conf);

    // 4. Assert the expected result
    assertEquals(EpollSocketChannel.class, actual);
  }
}