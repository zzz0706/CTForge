package alluxio.util;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class ConfigurationUtilsTest {

    @Test
    /**
     * Test case: testGetJobMasterRpcAddressesExplicitlyConfigured
     * Objective: Verify getJobMasterRpcAddresses correctly parses explicitly configured values for alluxio.job.master.rpc.addresses.
     * Prerequisites: The configuration key alluxio.job.master.rpc.addresses should be set in InstancedConfiguration.
     */
    public void testGetJobMasterRpcAddressesExplicitlyConfigured() {
        // 1. Initialize an InstancedConfiguration object using Alluxio APIs.
        AlluxioProperties props = new AlluxioProperties();
        InstancedConfiguration conf = new InstancedConfiguration(props);

        // 2. Prepare the test conditions: Explicitly configure alluxio.job.master.rpc.addresses with values.
        String configuredAddresses = "host1:19998,host2:19999";
        props.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, configuredAddresses);

        // 3. Call the method under test.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // 4. Assert the results match the expected values.
        Assert.assertEquals(2, addresses.size());
        Assert.assertTrue(addresses.contains(InetSocketAddress.createUnresolved("host1", 19998)));
        Assert.assertTrue(addresses.contains(InetSocketAddress.createUnresolved("host2", 19999)));
    }

    @Test
    /**
     * Test case: testGetJobMasterRpcAddressesFallbackToMasterRpcAddresses
     * Objective: Verify getJobMasterRpcAddresses falls back to parsing alluxio.master.rpc.addresses and overrides the port.
     * Prerequisites: The configuration key alluxio.job.master.rpc.addresses should NOT be set, but alluxio.master.rpc.addresses should be set in InstancedConfiguration.
     */
    public void testGetJobMasterRpcAddressesFallbackToMasterRpcAddresses() {
        // 1. Initialize an InstancedConfiguration object using Alluxio APIs.
        AlluxioProperties props = new AlluxioProperties();
        InstancedConfiguration conf = new InstancedConfiguration(props);

        // 2. Prepare the test conditions: Configure alluxio.master.rpc.addresses and network port derivation.
        props.set(PropertyKey.MASTER_RPC_ADDRESSES, "host3:19996,host4:19997");
        int jobRpcPort = ServerConfiguration.getInt(PropertyKey.JOB_MASTER_RPC_PORT);

        // 3. Call the method under test.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // 4. Assert the results: The port should be overridden to the derived Job Master RPC port.
        Assert.assertEquals(2, addresses.size());
        Assert.assertTrue(addresses.contains(InetSocketAddress.createUnresolved("host3", jobRpcPort)));
        Assert.assertTrue(addresses.contains(InetSocketAddress.createUnresolved("host4", jobRpcPort)));
    }

    @Test
    /**
     * Test case: testGetJobMasterRpcAddressesFallbackToEmbeddedJournalAddresses
     * Objective: Verify getJobMasterRpcAddresses falls back to parsing embedded journal addresses when neither job nor master RPC addresses are configured.
     * Prerequisites: Neither alluxio.job.master.rpc.addresses nor alluxio.master.rpc.addresses should be set in InstancedConfiguration.
     */
    public void testGetJobMasterRpcAddressesFallbackToEmbeddedJournalAddresses() {
        // 1. Initialize an InstancedConfiguration object using Alluxio APIs.
        AlluxioProperties props = new AlluxioProperties();
        InstancedConfiguration conf = new InstancedConfiguration(props);

        // 2. Prepare the test conditions: Leave job and master RPC addresses unconfigured.
        int jobRpcPort = ServerConfiguration.getInt(PropertyKey.JOB_MASTER_RPC_PORT);

        // 3. Call the method under test.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // 4. Assert the results: Derived with the overridden Job Master RPC port if embedded journal addresses are configured.
        Assert.assertFalse(addresses.isEmpty());
        for (InetSocketAddress address : addresses) {
            Assert.assertEquals(jobRpcPort, address.getPort());
        }
    }
}