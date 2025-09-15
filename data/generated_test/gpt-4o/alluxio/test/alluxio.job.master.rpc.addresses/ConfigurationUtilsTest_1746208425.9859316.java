package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class ConfigurationUtilsTest {

    @Test
    /**
     * Test case: test_getJobMasterRpcAddresses_noConfiguration
     * Objective: Verify the default fallback behavior when none of the relevant configuration properties are set.
     */
    public void test_getJobMasterRpcAddresses_noConfiguration() {
        // Prepare the test conditions.
        // Create an empty AlluxioProperties instance and use it to initialize the configuration.
        AlluxioProperties props = new AlluxioProperties();
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Ensure properties related to configuration are unset.
        Assert.assertFalse("Property 'alluxio.job.master.rpc.addresses' should not be set.",
                conf.isSet(PropertyKey.JOB_MASTER_RPC_ADDRESSES));
        Assert.assertFalse("Property 'alluxio.master.rpc.addresses' should not be set.",
                conf.isSet(PropertyKey.MASTER_RPC_ADDRESSES));
        
        // Test code.
        // Perform the call to retrieve Job Master RPC addresses.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Code after testing.
        // Validate that the returned list corresponds to sensible defaults or is empty.
        Assert.assertNotNull("Returned addresses list should not be null.", addresses);
        // The expected behavior is either sensible defaults derived from fallback logic or an empty list.
        if (addresses.isEmpty()) {
            System.out.println("No Job Master RPC addresses could be derived from the configuration.");
        } else {
            System.out.println("Job Master RPC addresses derived from fallback logic: " + addresses);
        }
    }

    @Test
    /**
     * Test case: Verify proper parsing when 'alluxio.job.master.rpc.addresses' configuration is explicitly set.
     */
    public void test_getJobMasterRpcAddresses_withJobMasterRpcAddressesConfigured() {
        // Prepare the test conditions.
        // Set up AlluxioProperties and explicitly set 'alluxio.job.master.rpc.addresses'.
        AlluxioProperties props = new AlluxioProperties();
        props.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "localhost:5001,127.0.0.1:5002");
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Test code.
        // Perform the call to retrieve Job Master RPC addresses.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Code after testing.
        // Validate that the returned list matches the explicitly set values.
        Assert.assertNotNull("Returned addresses list should not be null.", addresses);
        Assert.assertEquals("Expected exactly 2 addresses.", 2, addresses.size());
        Assert.assertEquals("Expected first address to match.", "localhost", addresses.get(0).getHostName());
        Assert.assertEquals("Expected first address port to match.", 5001, addresses.get(0).getPort());
        Assert.assertEquals("Expected second address to match.", "127.0.0.1", addresses.get(1).getHostName());
        Assert.assertEquals("Expected second address port to match.", 5002, addresses.get(1).getPort());
    }

    @Test
    /**
     * Test case: Verify fallback to 'alluxio.master.rpc.addresses' and port override when 'alluxio.job.master.rpc.addresses' is not set.
     */
    public void test_getJobMasterRpcAddresses_fallbackToMasterRpcAddresses() {
        // Prepare the test conditions.
        // Set up AlluxioProperties and explicitly set 'alluxio.master.rpc.addresses'.
        AlluxioProperties props = new AlluxioProperties();
        props.set(PropertyKey.MASTER_RPC_ADDRESSES, "localhost:6001,127.0.0.1:6002");
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Set the expected Job Master RPC port.
        props.set(PropertyKey.JOB_MASTER_RPC_PORT, "7001");

        // Test code.
        // Perform the call to retrieve Job Master RPC addresses.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Code after testing.
        // Validate that the returned list matches the master addresses with the overridden port.
        Assert.assertNotNull("Returned addresses list should not be null.", addresses);
        Assert.assertEquals("Expected exactly 2 addresses.", 2, addresses.size());
        Assert.assertEquals("Expected first address to match.", "localhost", addresses.get(0).getHostName());
        Assert.assertEquals("Expected first address port to be overridden.", 7001, addresses.get(0).getPort());
        Assert.assertEquals("Expected second address to match.", "127.0.0.1", addresses.get(1).getHostName());
        Assert.assertEquals("Expected second address port to be overridden.", 7001, addresses.get(1).getPort());
    }

    @Test
    /**
     * Test case: Verify fallback to embedded journal addresses when neither 'alluxio.job.master.rpc.addresses' nor 'alluxio.master.rpc.addresses' are set.
     */
    public void test_getJobMasterRpcAddresses_fallbackToEmbeddedJournalAddresses() {
        // Prepare the test conditions.
        // Set up AlluxioProperties and configure the embedded journal addresses.
        AlluxioProperties props = new AlluxioProperties();
        props.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, "localhost:8001,127.0.0.1:8002");
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Set the expected Job Master RPC port.
        props.set(PropertyKey.JOB_MASTER_RPC_PORT, "9001");

        // Test code.
        // Perform the call to retrieve Job Master RPC addresses.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Code after testing.
        // Validate that the returned list matches the embedded journal addresses with the overridden port.
        Assert.assertNotNull("Returned addresses list should not be null.", addresses);
        Assert.assertEquals("Expected exactly 2 addresses.", 2, addresses.size());
        Assert.assertEquals("Expected first address to match.", "localhost", addresses.get(0).getHostName());
        Assert.assertEquals("Expected first address port to be overridden.", 9001, addresses.get(0).getPort());
        Assert.assertEquals("Expected second address to match.", "127.0.0.1", addresses.get(1).getHostName());
        Assert.assertEquals("Expected second address port to be overridden.", 9001, addresses.get(1).getPort());
    }
}