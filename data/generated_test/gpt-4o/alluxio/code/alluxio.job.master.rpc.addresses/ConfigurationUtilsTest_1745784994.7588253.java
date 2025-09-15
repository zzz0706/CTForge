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

    /**
     * Test case: testGetJobMasterRpcAddressesFallbackToEmbeddedJournalAddresses
     * Objective: Check fallback behavior when neither alluxio.job.master.rpc.addresses 
     * nor alluxio.master.rpc.addresses is set.
     */
    @Test
    public void testGetJobMasterRpcAddressesFallbackToEmbeddedJournalAddresses() {
        // Prepare the test conditions.
        // 1. Initialize the AlluxioConfiguration.
        AlluxioConfiguration conf = ServerConfiguration.global();

        // Ensure embedded journal addresses are configured for the fallback logic.
        List<InetSocketAddress> embeddedJournalAddresses = ConfigurationUtils.getEmbeddedJournalAddresses(conf, NetworkAddressUtils.ServiceType.JOB_MASTER_RAFT);
        Assert.assertNotNull("Embedded journal addresses should exist", embeddedJournalAddresses);
        Assert.assertFalse("Embedded journal addresses should not be empty", embeddedJournalAddresses.isEmpty());

        // Set the expected Job Master RPC port dynamically based on the configuration.
        int jobMasterRpcPort = NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC, conf);
        Assert.assertTrue("Job Master RPC port should be valid", jobMasterRpcPort > 0);

        // Test the fallback logic using the function under test.
        List<InetSocketAddress> jobMasterRpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Verify that the returned addresses are from the embedded journal, with the correct overridden RPC port.
        Assert.assertEquals("The size of the returned list should match the number of embedded journal addresses",
                embeddedJournalAddresses.size(), jobMasterRpcAddresses.size());
        for (int i = 0; i < embeddedJournalAddresses.size(); i++) {
            InetSocketAddress embeddedJournalAddress = embeddedJournalAddresses.get(i);
            InetSocketAddress jobMasterRpcAddress = jobMasterRpcAddresses.get(i);

            Assert.assertEquals("Hostnames should match", embeddedJournalAddress.getHostName(), jobMasterRpcAddress.getHostName());
            Assert.assertEquals("Port should match the overridden Job Master RPC port", jobMasterRpcPort, jobMasterRpcAddress.getPort());
        }
    }
}