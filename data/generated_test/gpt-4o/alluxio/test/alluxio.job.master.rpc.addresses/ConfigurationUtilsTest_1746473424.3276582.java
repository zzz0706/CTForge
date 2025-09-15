package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class ConfigurationUtilsTest {

    /**
     * Test case: testFallbackToMasterRpcAddressesWithOverridePort
     * - Ensure the fallback behavior of job master RPC addresses works correctly when an override port is provided.
     */
    @Test
    public void testFallbackToMasterRpcAddressesWithOverridePort() {
        // Step 1: Use InstancedConfiguration to create a non-abstract AlluxioConfiguration instance.
        InstancedConfiguration config = InstancedConfiguration.defaults();

        // Step 2: Add PropertyKey.MASTER_RPC_ADDRESSES in the configuration using the correct API.
        String masterRpcAddresses = "masterHost1:2000,masterHost2:2000";
        config.set(PropertyKey.MASTER_RPC_ADDRESSES, masterRpcAddresses);

        // Step 3: Add the Job Master RPC port in the configuration via PropertyKey.JOB_MASTER_RPC_PORT.
        int jobMasterRpcPort = 4000;
        config.set(PropertyKey.JOB_MASTER_RPC_PORT, String.valueOf(jobMasterRpcPort));

        // Step 4: Invoke the method ConfigurationUtils.getJobMasterRpcAddresses with the correct configuration.
        List<InetSocketAddress> jobMasterAddresses = ConfigurationUtils.getJobMasterRpcAddresses(config);

        // Step 5: Verify the returned List<InetSocketAddress> matches the expected values.
        Assert.assertEquals(2, jobMasterAddresses.size());
        Assert.assertEquals("masterHost1", jobMasterAddresses.get(0).getHostName());
        Assert.assertEquals(jobMasterRpcPort, jobMasterAddresses.get(0).getPort());
        Assert.assertEquals("masterHost2", jobMasterAddresses.get(1).getHostName());
        Assert.assertEquals(jobMasterRpcPort, jobMasterAddresses.get(1).getPort());
    }
}