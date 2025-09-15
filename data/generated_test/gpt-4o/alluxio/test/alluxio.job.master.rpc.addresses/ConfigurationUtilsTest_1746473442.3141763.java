package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class ConfigurationUtilsTest {

    @Test
    // Test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testFallbackToMasterRpcAddressesWithOverridePort() {
        // Step 1: Create an AlluxioConfiguration instance using InstancedConfiguration.
        InstancedConfiguration config = InstancedConfiguration.defaults();

        // Step 2: Set configuration values using PropertyKey.
        String masterRpcAddresses = "masterHost1:2000,masterHost2:2000";
        config.set(PropertyKey.MASTER_RPC_ADDRESSES, masterRpcAddresses);
        
        int jobMasterRpcPort = 4000;
        config.set(PropertyKey.JOB_MASTER_RPC_PORT, String.valueOf(jobMasterRpcPort));

        // Step 3: Invoke the method to retrieve the expected job master RPC addresses.
        List<InetSocketAddress> jobMasterAddresses = ConfigurationUtils.getJobMasterRpcAddresses(config);

        // Step 4: Validate that the returned addresses correctly override the port.
        Assert.assertEquals(2, jobMasterAddresses.size());
        Assert.assertEquals("masterHost1", jobMasterAddresses.get(0).getHostName());
        Assert.assertEquals(jobMasterRpcPort, jobMasterAddresses.get(0).getPort());
        Assert.assertEquals("masterHost2", jobMasterAddresses.get(1).getHostName());
        Assert.assertEquals(jobMasterRpcPort, jobMasterAddresses.get(1).getPort());
    }

    @Test
    public void testExplicitJobMasterRpcAddresses() {
        // Step 1: Create an AlluxioConfiguration instance using InstancedConfiguration.
        InstancedConfiguration config = InstancedConfiguration.defaults();

        // Step 2: Set explicit job master RPC addresses in the configuration.
        String jobMasterRpcAddresses = "explicitHost1:3000,explicitHost2:3000";
        config.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, jobMasterRpcAddresses);

        // Step 3: Invoke the method to retrieve the job master RPC addresses.
        List<InetSocketAddress> jobMasterAddresses = ConfigurationUtils.getJobMasterRpcAddresses(config);

        // Step 4: Validate that the returned addresses match the explicit configuration.
        Assert.assertEquals(2, jobMasterAddresses.size());
        Assert.assertEquals("explicitHost1", jobMasterAddresses.get(0).getHostName());
        Assert.assertEquals(3000, jobMasterAddresses.get(0).getPort());
        Assert.assertEquals("explicitHost2", jobMasterAddresses.get(1).getHostName());
        Assert.assertEquals(3000, jobMasterAddresses.get(1).getPort());
    }

    @Test
    public void testFallbackToEmbeddedJournalAddressesWithOverridePort() {
        // Step 1: Create an AlluxioConfiguration instance using InstancedConfiguration.
        InstancedConfiguration config = InstancedConfiguration.defaults();

        // Step 2: Set the Job Master RPC port in the configuration.
        int jobMasterRpcPort = 5000;
        config.set(PropertyKey.JOB_MASTER_RPC_PORT, String.valueOf(jobMasterRpcPort));

        // Step 3: Ensure neither job master RPC nor master RPC addresses are configured.
        config.unset(PropertyKey.JOB_MASTER_RPC_ADDRESSES);
        config.unset(PropertyKey.MASTER_RPC_ADDRESSES);

        // Step 4: Invoke the method to retrieve the Job Master RPC addresses.
        List<InetSocketAddress> jobMasterAddresses = ConfigurationUtils.getJobMasterRpcAddresses(config);

        // Step 5: Validate fallback behavior to embedded journal addresses with overridden port.
        for (InetSocketAddress addr : jobMasterAddresses) {
            Assert.assertEquals(jobMasterRpcPort, addr.getPort());
        }
    }
}