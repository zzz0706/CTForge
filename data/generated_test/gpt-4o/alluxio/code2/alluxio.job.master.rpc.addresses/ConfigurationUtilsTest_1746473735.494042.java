package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Test;

import java.util.List;
import java.net.InetSocketAddress;

import static org.junit.Assert.*;

public class ConfigurationUtilsTest {

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidJobMasterRpcAddressesFormat() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        InstancedConfiguration configuration = InstancedConfiguration.defaults();
        
        // 2. Prepare the test conditions: Set an invalid format for PropertyKey.JOB_MASTER_RPC_ADDRESSES.
        configuration.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1,host2:abcd");

        // 3. Test code: Call the method getJobMasterRpcAddresses which should throw an IllegalArgumentException.
        List<InetSocketAddress> rpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(configuration);

        // 4. Verify that no addresses are returned after exception handling (execution should not reach here).
        assertTrue(rpcAddresses.isEmpty());
    }
}