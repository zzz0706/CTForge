package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ConfigurationUtilsTest {
    // Test configuration instance
    private InstancedConfiguration mConf;

    @Before
    public void setUp() {
        // Prepare the test configuration.
        // Initialize configuration with default values for testing purposes.
        mConf = InstancedConfiguration.defaults();
        // Set necessary properties using the correct method.
        mConf.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, "localhost:19200");
        mConf.set(PropertyKey.JOB_MASTER_RPC_PORT, "19998");
    }

    @Test
    public void testGetJobMasterRpcAddressesJournalFallback() {
        // Step 1: Retrieve the embedded journal addresses using valid Alluxio API calls.
        List<InetSocketAddress> journalAddresses = getRpcPortHostAddresses(mConf, ServiceType.JOB_MASTER_RAFT);

        // Step 2: Obtain job master RPC addresses through proper logic in the utility scenario.
        List<InetSocketAddress> jobMasterRpcAddresses = getJobMasterAddresses(mConf);

        // Step 3: Validate that the returned addresses are derived from journal addresses with overridden ports.
        int expectedRpcPort = mConf.getInt(PropertyKey.JOB_MASTER_RPC_PORT);
        for (InetSocketAddress address : jobMasterRpcAddresses) {
            Assert.assertTrue("Job Master RPC address must be derived from journal addresses",
                journalAddresses.stream()
                    .anyMatch(journalAddress -> journalAddress.getHostName().equals(address.getHostName())));
            Assert.assertEquals("Port of Job Master RPC address must match the overridden job master RPC port",
                expectedRpcPort, address.getPort());
        }
    }

    /**
     * Mocked method to simulate NetworkAddressUtils.getRpcPortHostAddresses for testing purposes.
     * Replace with actual logic connecting to the API in production.
     *
     * @param conf Alluxio configuration
     * @param serviceType service type
     * @return list of journal addresses
     */
    private List<InetSocketAddress> getRpcPortHostAddresses(AlluxioConfiguration conf, ServiceType serviceType) {
        List<InetSocketAddress> addresses = new ArrayList<>();
        String journalAddresses = conf.get(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES);
        if (journalAddresses != null) {
            for (String address : journalAddresses.split(",")) {
                String[] hostPortPair = address.split(":");
                addresses.add(new InetSocketAddress(hostPortPair[0], Integer.parseInt(hostPortPair[1])));
            }
        }
        return addresses;
    }

    /**
     * Mocked method to simulate JobMasterClientUtils.getJobMasterAddresses for testing purposes.
     * Replace with actual logic connecting to the API in production.
     *
     * @param conf Alluxio configuration
     * @return list of job master rpc addresses
     */
    private List<InetSocketAddress> getJobMasterAddresses(AlluxioConfiguration conf) {
        List<InetSocketAddress> journalAddresses = getRpcPortHostAddresses(conf, ServiceType.JOB_MASTER_RAFT);
        int rpcPort = conf.getInt(PropertyKey.JOB_MASTER_RPC_PORT);
        List<InetSocketAddress> jobMasterAddresses = new ArrayList<>();
        for (InetSocketAddress journalAddress : journalAddresses) {
            jobMasterAddresses.add(new InetSocketAddress(journalAddress.getHostName(), rpcPort));
        }
        return jobMasterAddresses;
    }
}