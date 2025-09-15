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
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetJobMasterRpcAddressesFallbackToEmbeddedJournalAddresses() {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values.
        // This is done during initialization by leveraging the ServerConfiguration.global() API.

        // 2. Prepare the test conditions.

        // Create an AlluxioConfiguration object to retrieve and modify configurations during the test.
        AlluxioConfiguration conf = ServerConfiguration.global();

        // Verify that the embedded journal addresses are configured. This ensures the fallback logic can be tested.
        List<InetSocketAddress> embeddedJournalAddresses = ConfigurationUtils.getEmbeddedJournalAddresses(
            conf, ServiceType.JOB_MASTER_RAFT);
        Assert.assertNotNull("Embedded journal addresses should be configured", embeddedJournalAddresses);
        Assert.assertFalse("Embedded journal addresses should not be empty", embeddedJournalAddresses.isEmpty());

        // Dynamically retrieve the Job Master RPC port using Alluxio utilities.
        int jobMasterRpcPort = NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RPC, conf);
        Assert.assertTrue("Job Master RPC port must be a valid positive number", jobMasterRpcPort > 0);

        // 3. Test code.

        // Call the tested API to retrieve the Job Master RPC addresses.
        List<InetSocketAddress> jobMasterRpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Validate the fallback behavior.
        // The size of the returned list of Job Master RPC addresses should match the size of the embedded journal addresses list.
        Assert.assertEquals("Mismatch in count between fallback Job Master RPC addresses and embedded journal addresses",
                embeddedJournalAddresses.size(), jobMasterRpcAddresses.size());

        // Verify that each address in the Job Master RPC list corresponds to the appropriate fallback configuration.
        for (int i = 0; i < embeddedJournalAddresses.size(); i++) {
            InetSocketAddress embeddedJournalAddress = embeddedJournalAddresses.get(i);
            InetSocketAddress jobMasterRpcAddress = jobMasterRpcAddresses.get(i);

            // Check that the hostnames from the embedded journal entries are correctly carried over to Job Master RPC addresses.
            Assert.assertEquals("Hostnames should match for fallback Job Master RPC addresses",
                    embeddedJournalAddress.getHostName(), jobMasterRpcAddress.getHostName());

            // Confirm that the Job Master RPC port is correctly applied to the resulting addresses.
            Assert.assertEquals("Ports should be updated to the Job Master RPC port",
                    jobMasterRpcPort, jobMasterRpcAddress.getPort());
        }

        // 4. Code after testing (optional).
        // Since this test does not involve modifications to the runtime configuration or system states, no cleanup is required.
        // Additional logs or assertions could be added here if needed for debugging or validation.
    }
}