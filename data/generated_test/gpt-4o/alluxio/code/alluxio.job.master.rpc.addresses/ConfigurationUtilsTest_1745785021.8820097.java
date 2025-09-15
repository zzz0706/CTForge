package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
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
    // 1. Utilize the Alluxio 2.1.0 API correctly to obtain configuration values dynamically.
    // 2. Prepare the test conditions.
    // 3. Test the behavior of fallback logic for Job Master RPC addresses.
    // 4. Code after testing (if needed for cleanup or additional checks).
    public void testGetJobMasterRpcAddressesFallbackToEmbeddedJournalAddresses() {
        // 2. Prepare the test conditions.

        // Create a global AlluxioConfiguration object.
        AlluxioConfiguration conf = ServerConfiguration.global();

        // Validate that the embedded journal addresses are configured.
        List<InetSocketAddress> embeddedJournalAddresses = ConfigurationUtils.getEmbeddedJournalAddresses(
            conf, ServiceType.JOB_MASTER_RAFT);
        Assert.assertNotNull("Embedded journal addresses should exist", embeddedJournalAddresses);
        Assert.assertFalse("Embedded journal addresses should not be empty", embeddedJournalAddresses.isEmpty());

        // Retrieve the Job Master RPC port dynamically from the configuration.
        int jobMasterRpcPort = NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RPC, conf);
        Assert.assertTrue("Job Master RPC port should be valid", jobMasterRpcPort > 0);

        // 3. Test code - Invoke the public method and validate its result.

        // Get the Job Master RPC addresses using the `getJobMasterRpcAddresses` API.
        List<InetSocketAddress> jobMasterRpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Verify that the fallback addresses match the configuration of the embedded journal addresses.
        Assert.assertEquals("The number of fallback addresses should match embedded journal addresses count",
                embeddedJournalAddresses.size(), jobMasterRpcAddresses.size());

        // Validate that the hostnames and ports match between the embedded entries and the Job Master RPC configuration.
        for (int i = 0; i < embeddedJournalAddresses.size(); i++) {
            InetSocketAddress embeddedJournalAddress = embeddedJournalAddresses.get(i);
            InetSocketAddress jobMasterRpcAddress = jobMasterRpcAddresses.get(i);

            // Check if the hostname matches.
            Assert.assertEquals("Hostnames should match", embeddedJournalAddress.getHostName(), jobMasterRpcAddress.getHostName());
            
            // Check if the port matches the dynamically fetched Job Master RPC port.
            Assert.assertEquals("Ports should match the configured Job Master RPC port",
                    jobMasterRpcPort, jobMasterRpcAddress.getPort());
        }

        // 4. Code after testing (optional, if there is any cleanup or additional logging/validation).
        // Note: No explicit cleanup is required in this test case context.
    }
}