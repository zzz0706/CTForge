package alluxio.util.network;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class NettyUtilsConfigValidationTest {

  @Test
  public void validateUserNetworkNettyChannel() {
    // 1. Load configuration from alluxio-site.properties (no value set in test code)
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare test conditions – no explicit set, rely on file or default
    // 3. Test code – read the value and validate
    String value = conf.get(PropertyKey.USER_NETWORK_NETTY_CHANNEL);

    // 4. Validate against allowed enum values
    assertTrue("Invalid alluxio.user.network.netty.channel value: " + value,
        value.equalsIgnoreCase("EPOLL") || value.equalsIgnoreCase("NIO"));
  }
}