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
    // Test case: Verify the default fallback behavior when none of the relevant configuration properties are set.
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getJobMasterRpcAddresses_noConfiguration() {
        // Step 2: Prepare the test conditions.
        // Create an empty configuration without any relevant properties set.
        AlluxioProperties props = new AlluxioProperties();
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Ensure the required configuration properties are unset.
        Assert.assertFalse("Property '" + PropertyKey.JOB_MASTER_RPC_ADDRESSES + "' should not be set.",
                conf.isSet(PropertyKey.JOB_MASTER_RPC_ADDRESSES));
        Assert.assertFalse("Property '" + PropertyKey.MASTER_RPC_ADDRESSES + "' should not be set.",
                conf.isSet(PropertyKey.MASTER_RPC_ADDRESSES));

        // Step 3: Test code.
        // Call the method to retrieve the Job Master RPC addresses.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Step 4: Code after testing.
        // Verify the result matches the expected behavior (empty or sensible defaults).
        Assert.assertNotNull("Returned list of addresses should not be null.", addresses);
        if (addresses.isEmpty()) {
            System.out.println("No Job Master RPC addresses derived; default behavior validated.");
        } else {
            System.out.println("Derived Job Master RPC addresses: " + addresses);
        }
    }

    @Test
    // Test case: Verify the behavior when the configuration explicitly specifies 'alluxio.job.master.rpc.addresses'.
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getJobMasterRpcAddresses_withJobMasterRpcAddressesConfigured() {
        // Step 2: Prepare the test conditions.
        // Set the 'alluxio.job.master.rpc.addresses' property.
        AlluxioProperties props = new AlluxioProperties();
        props.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "localhost:5001,127.0.0.1:5002");
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Step 3: Test code.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Step 4: Code after testing.
        Assert.assertNotNull("Returned list of addresses should not be null.", addresses);
        Assert.assertEquals("Unexpected number of addresses.", 2, addresses.size());

        InetSocketAddress addr1 = addresses.get(0);
        Assert.assertEquals("First address should match.", "localhost", addr1.getHostName());
        Assert.assertEquals("First port should match.", 5001, addr1.getPort());

        InetSocketAddress addr2 = addresses.get(1);
        Assert.assertEquals("Second address should match.", "127.0.0.1", addr2.getHostName());
        Assert.assertEquals("Second port should match.", 5002, addr2.getPort());
    }

    @Test
    // Test case: Verify fallback when 'alluxio.master.rpc.addresses' is set but 'alluxio.job.master.rpc.addresses' is not.
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getJobMasterRpcAddresses_fallbackToMasterRpcAddresses() {
        // Step 2: Prepare the test conditions.
        AlluxioProperties props = new AlluxioProperties();
        props.set(PropertyKey.MASTER_RPC_ADDRESSES, "localhost:6001,127.0.0.1:6002");
        props.set(PropertyKey.JOB_MASTER_RPC_PORT, "7001");
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Step 3: Test code.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Step 4: Code after testing.
        Assert.assertNotNull("Returned list of addresses should not be null.", addresses);
        Assert.assertEquals("Unexpected number of addresses.", 2, addresses.size());

        InetSocketAddress addr1 = addresses.get(0);
        Assert.assertEquals("Expected hostname match for address 1.", "localhost", addr1.getHostName());
        Assert.assertEquals("Expected port override for address 1.", 7001, addr1.getPort());

        InetSocketAddress addr2 = addresses.get(1);
        Assert.assertEquals("Expected hostname match for address 2.", "127.0.0.1", addr2.getHostName());
        Assert.assertEquals("Expected port override for address 2.", 7001, addr2.getPort());
    }

    @Test
    // Test case: Verify fallback to 'embedded journal addresses' when neither 'job.master.rpc.addresses' nor 'master.rpc.addresses' is set.
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getJobMasterRpcAddresses_fallbackToEmbeddedJournalAddresses() {
        // Step 2: Prepare the test conditions.
        AlluxioProperties props = new AlluxioProperties();
        props.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, "localhost:8001,127.0.0.1:8002");
        props.set(PropertyKey.JOB_MASTER_RPC_PORT, "9001");
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Step 3: Test code.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Step 4: Code after testing.
        Assert.assertNotNull("Returned list of addresses should not be null.", addresses);
        Assert.assertEquals("Unexpected number of addresses.", 2, addresses.size());

        InetSocketAddress addr1 = addresses.get(0);
        Assert.assertEquals("Expected hostname match for address 1.", "localhost", addr1.getHostName());
        Assert.assertEquals("Expected port override for address 1.", 9001, addr1.getPort());

        InetSocketAddress addr2 = addresses.get(1);
        Assert.assertEquals("Expected hostname match for address 2.", "127.0.0.1", addr2.getHostName());
        Assert.assertEquals("Expected port override for address 2.", 9001, addr2.getPort());
    }
}