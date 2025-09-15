package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Test;

import java.util.List;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertTrue;

public class ConfigurationUtilsTest {

    /**
     * Test method to validate behavior when PropertyKey.JOB_MASTER_RPC_ADDRESSES is set with an invalid format.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidJobMasterRpcAddressesFormat() {
        // 1. Prepare the test conditions with a valid AlluxioConfiguration initialization.
        InstancedConfiguration configuration = InstancedConfiguration.defaults();

        // 2. Set an invalid format for PropertyKey.JOB_MASTER_RPC_ADDRESSES using the correct API.
        configuration.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1,host2:abcd");

        // 3. Get job master RPC addresses, expecting an IllegalArgumentException due to invalid format.
        List<InetSocketAddress> rpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(configuration);

        // The test expects an exception to be thrown and does not reach this point.
        // Adding a sanity check to ensure exception handling worked.
        assertTrue(rpcAddresses.isEmpty());
    }
}