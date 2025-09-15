package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class ConfigurationUtilsTest {

    @Test
    /**
     * Test case: testGetJobMasterRpcAddressesFallbackToEmbeddedJournalAddresses
     * Objective: Verify fallback behavior when neither alluxio.job.master.rpc.addresses 
     * nor alluxio.master.rpc.addresses is explicitly set.
     */
    public void testGetJobMasterRpcAddressesFallbackToEmbeddedJournalAddresses() {
        // 1. Prepare the test conditions.
        AlluxioConfiguration conf = ServerConfiguration.global();

        // Ensure embedded journal addresses are available and valid.
        List<InetSocketAddress> embeddedJournalAddresses = ConfigurationUtils.getEmbeddedJournalAddresses(
            conf, NetworkAddressUtils.ServiceType.JOB_MASTER_RAFT);
        Assert.assertNotNull("Embedded journal addresses should exist", embeddedJournalAddresses);
        Assert.assertFalse("Embedded journal addresses should not be empty", embeddedJournalAddresses.isEmpty());

        // Fetch the dynamically configured Job Master RPC port from the configuration.
        int jobMasterRpcPort = NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC, conf);
        Assert.assertTrue("Job Master RPC port should be valid", jobMasterRpcPort > 0);

        // 2. Test code - Call the method under test.
        List<InetSocketAddress> jobMasterRpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // 3. Assertions - Validate the behavior and results.
        Assert.assertEquals("Fallback addresses should match embedded journal addresses count",
                embeddedJournalAddresses.size(), jobMasterRpcAddresses.size());
        for (int i = 0; i < embeddedJournalAddresses.size(); i++) {
            InetSocketAddress embeddedJournalAddress = embeddedJournalAddresses.get(i);
            InetSocketAddress jobMasterRpcAddress = jobMasterRpcAddresses.get(i);

            // Verify the hostname matches the embedded journal address.
            Assert.assertEquals("Hostnames should match", embeddedJournalAddress.getHostName(), jobMasterRpcAddress.getHostName());
            
            // Verify the port matches the dynamically fetched Job Master RPC port.
            Assert.assertEquals("Ports should match the configured Job Master RPC port", 
                    jobMasterRpcPort, jobMasterRpcAddress.getPort());
        }

        // 4. Code after testing - Cleanup or additional logging could go here, if necessary.
        // Note: No explicit cleanup is necessary in this context.
    }
}