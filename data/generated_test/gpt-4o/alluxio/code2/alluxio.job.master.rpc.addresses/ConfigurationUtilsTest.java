package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConfigurationUtilsTest {
    /**
     * Test method for verifying that `getJobMasterRpcAddresses` correctly parses explicitly configured
     * job master RPC addresses from the configuration.
     */
    @Test
    public void testExplicitJobMasterRpcAddressesConfigured() {
        // 1. Prepare test conditions: Create an AlluxioProperties instance and initialize it.
        AlluxioProperties properties = new AlluxioProperties();

        // 2. Set PropertyKey.JOB_MASTER_RPC_ADDRESSES to a list of valid `host:port` pairs.
        properties.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1:1234,host2:5678");

        // 3. Create an InstancedConfiguration instance using the AlluxioProperties.
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // 4. Invoke the `getJobMasterRpcAddresses` method with the configuration.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(configuration);

        // 5. Test code: Assert that the method returns the expected `InetSocketAddress` list.
        assertEquals(2, addresses.size());
        assertEquals("host1", addresses.get(0).getHostName());
        assertEquals(1234, addresses.get(0).getPort());
        assertEquals("host2", addresses.get(1).getHostName());
        assertEquals(5678, addresses.get(1).getPort());
    }
}