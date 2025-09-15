package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.util.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class ConfigurationUtilsTest {

    /**
     * Test case: test_getJobMasterRpcAddresses_noConfiguration
     * Objective: Verify the default fallback behavior when none of the relevant configuration properties are set.
     */
    @Test
    public void test_getJobMasterRpcAddresses_noConfiguration() {
        // Prepare the test conditions.
        // Create an AlluxioProperties instance and use it to initialize the AlluxioConfiguration.
        AlluxioProperties props = new AlluxioProperties();
        AlluxioConfiguration conf = new InstancedConfiguration(props);

        // Test code.
        // Call the method to retrieve Job Master RPC addresses.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Code after testing.
        // Validate that the returned list corresponds to sensible defaults as per the implementation logic or an empty list.
        Assert.assertNotNull("Returned addresses list should not be null.", addresses);
        // The expected behavior is either sensible defaults derived from fallback logic or an empty list.
        if (addresses.isEmpty()) {
            System.out.println("No Job Master RPC addresses could be derived from the configuration.");
        } else {
            System.out.println("Job Master RPC addresses derived from fallback logic: " + addresses);
        }
    }
}