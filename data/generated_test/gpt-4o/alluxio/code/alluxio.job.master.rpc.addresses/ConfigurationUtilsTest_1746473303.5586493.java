package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConfigurationUtilsTest {

    @Test
    public void testExplicitJobMasterRpcAddressesConfigured() {
        // 1. Prepare the test conditions: create and configure an AlluxioProperties instance.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1:1234,host2:5678");

        // Create an InstancedConfiguration instance using the AlluxioProperties.
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // 2. Invoke the `getJobMasterRpcAddresses` method with the configured properties.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(configuration);

        // 3. Test code: Assert that the method returns the expected list of `InetSocketAddress` objects.
        assertEquals(2, addresses.size());
        assertEquals("host1", addresses.get(0).getHostName());
        assertEquals(1234, addresses.get(0).getPort());
        assertEquals("host2", addresses.get(1).getHostName());
        assertEquals(5678, addresses.get(1).getPort());
    }

    @Test
    public void testFallbackToMasterRpcAddresses() {
        // 1. Prepare the test conditions: configuring fallback to MASTER_RPC_ADDRESSES with overridden port.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.MASTER_RPC_ADDRESSES, "host3:1234,host4:5678");
        properties.set(PropertyKey.JOB_MASTER_RPC_PORT, "9999");

        // Create an InstancedConfiguration instance using the AlluxioProperties.
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // 2. Invoke the `getJobMasterRpcAddresses` method with fallback settings.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(configuration);

        // 3. Test code: Assert that the master RPC addresses are reused with overridden ports.
        assertEquals(2, addresses.size());
        assertEquals("host3", addresses.get(0).getHostName());
        assertEquals(9999, addresses.get(0).getPort());
        assertEquals("host4", addresses.get(1).getHostName());
        assertEquals(9999, addresses.get(1).getPort());
    }

    @Test
    public void testFallbackToEmbeddedJournalAddresses() {
        // 1. Prepare the test conditions: simulate configuration for fallback to embedded journal addresses.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.JOB_MASTER_RPC_PORT, "8888");

        // Create an InstancedConfiguration instance using the AlluxioProperties.
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // 2. Invoke the `getJobMasterRpcAddresses` method when no explicit addresses are set.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(configuration);

        // 3. Test code: Validate fallback logic. Compare with expected embedded journal address.
        assertEquals(1, addresses.size());  // Assuming the fallback returns a single embedded journal address.
        assertEquals("10.10.0.11", addresses.get(0).getHostName()); // Dynamically verify the expected embedded journal host.
        assertEquals(8888, addresses.get(0).getPort()); // Validate that the overridden port is correctly applied.
    }
}