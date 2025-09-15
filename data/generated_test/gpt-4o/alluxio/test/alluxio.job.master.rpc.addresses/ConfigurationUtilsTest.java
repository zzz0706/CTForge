package alluxio.util;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConfigurationUtilsTest {

    @Test
    public void testGetJobMasterRpcAddressesExplicitConfiguration() {
        // Prepare the test environment
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1:1234,host2:5678");
        InstancedConfiguration configuration = new InstancedConfiguration(properties);

        // Use Alluxio API to obtain configuration values
        List<InetSocketAddress> result = ConfigurationUtils.getJobMasterRpcAddresses(configuration);

        // Expected result
        List<InetSocketAddress> expectedAddresses = Arrays.asList(
            new InetSocketAddress("host1", 1234),
            new InetSocketAddress("host2", 5678)
        );

        // Perform test assertions
        assertEquals(expectedAddresses, result);

        // Code after testing (clean-up if necessary)
    }
}