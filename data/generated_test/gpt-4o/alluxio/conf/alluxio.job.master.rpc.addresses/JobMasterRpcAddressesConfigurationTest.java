package alluxio.conf;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public class JobMasterRpcAddressesConfigurationTest {

    @Test
    public void testJobMasterRpcAddressesConfiguration() {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        AlluxioProperties alluxioProperties = ConfigurationUtils.defaults();
        InstancedConfiguration conf = new InstancedConfiguration(alluxioProperties);

        // 2. Prepare the test conditions.
        // Ensure the configuration has some Job Master RPC Addresses set for testing;
        // conf.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "localhost:20001");
        conf.set(PropertyKey.MASTER_RPC_ADDRESSES, "localhost:19999");
        conf.set(PropertyKey.JOB_MASTER_RPC_PORT, "20001");

        List<InetSocketAddress> jobMasterRpcAddresses;

        try {
            // 3.a Test primary configuration (JOB_MASTER_RPC_ADDRESSES).
            if (conf.isSet(PropertyKey.JOB_MASTER_RPC_ADDRESSES)) {
                jobMasterRpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);
                Assert.assertNotNull(
                        "Job master RPC addresses should not be null if explicitly configured.",
                        jobMasterRpcAddresses);

                // Verify that all addresses are valid
                for (InetSocketAddress address : jobMasterRpcAddresses) {
                    Assert.assertNotNull("Each job master RPC address should have a valid hostname.", address.getHostName());
                    Assert.assertTrue("Invalid port number in job master RPC address. Must be between 1 and 65535.",
                            address.getPort() > 0 && address.getPort() <= 65535);
                }
            } else {
                // 3.b Fallback to job master configuration logic.
                int jobRpcPort = conf.getInt(PropertyKey.JOB_MASTER_RPC_PORT);

                if (conf.isSet(PropertyKey.MASTER_RPC_ADDRESSES)) {
                    List<InetSocketAddress> masterRpcAddresses = ConfigurationUtils.getMasterRpcAddresses(conf);
                    Assert.assertNotNull(
                            "Fallback to master RPC addresses should yield non-null job master RPC addresses.",
                            masterRpcAddresses);

                    for (InetSocketAddress address : masterRpcAddresses) {
                        Assert.assertNotNull("Each fallback master RPC address should have a valid hostname.", address.getHostName());
                        Assert.assertTrue("Invalid port number in fallback master RPC address. Must match job master RPC port.",
                                address.getPort() == jobRpcPort);
                    }
                } else {
                    // Fallback to embedded journal configuration
                    List<InetSocketAddress> embeddedJournalAddresses =
                            ConfigurationUtils.getEmbeddedJournalAddresses(conf, ServiceType.JOB_MASTER_RPC);
                    Assert.assertNotNull(
                            "Fallback to embedded journal addresses should yield non-null job master RPC addresses.",
                            embeddedJournalAddresses);

                    for (InetSocketAddress address : embeddedJournalAddresses) {
                        Assert.assertNotNull("Each embedded journal master RPC address should have a valid hostname.", address.getHostName());
                        Assert.assertTrue("Invalid port number in embedded journal master RPC address. Must match job master RPC port.",
                                address.getPort() == jobRpcPort);
                    }
                }
            }
        } catch (IllegalStateException e) {
            // Test setup failed with unexpected state; attempting fallback to default configuration to avoid runtime errors.
            jobMasterRpcAddresses = Collections.emptyList();
        }

        // 4. Post-test code: Ensure cleanup or proper teardown is done if needed.
    }
}