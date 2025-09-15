package alluxio.util.network;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.util.network.NettyUtils;
import org.junit.Assert;
import org.junit.Test;

public class NettyUtilsTest {

    /**
     * This test validates the behavior of the NettyUtils.getUserChannel method
     * when provided with valid configuration.
     */
    @Test
    public void test_getUserChannel_with_valid_configuration() {
        // 1. Prepare the required Alluxio configuration using its APIs
        AlluxioProperties properties = new AlluxioProperties();
        AlluxioConfiguration baseConf = new InstancedConfiguration(properties);

        // Set specific properties dynamically for the test scope
        properties.set(PropertyKey.USER_NETWORK_NETTY_CHANNEL, "NIO");

        // 2. Test code: invoke the method under test
        ChannelType result = NettyUtils.getUserChannel(baseConf);

        // 3. Validate if the returned channel matches the configured value
        Assert.assertNotNull(result);
        Assert.assertEquals(ChannelType.NIO, result);

        // 4. Post-test cleanup is not required because configurations
        // were set dynamically and are limited to the test scope.
    }
}