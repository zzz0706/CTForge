package alluxio.util;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class ConfigurationUtilsTest {
    /**
     * Test case: test_getJobMasterRpcAddresses_masterRpcFallback
     * Objective: Verify the fallback logic when 'alluxio.job.master.rpc.addresses' is not explicitly configured
     * but 'alluxio.master.rpc.addresses' is set.
     */
    @Test
    public void test_getJobMasterRpcAddresses_masterRpcFallback() {
        // Prepare the test configuration by removing any overriding values
        ServerConfiguration.reset(); // Corrected method to clear the configuration

        // Dynamically set specific properties
        ServerConfiguration.set(PropertyKey.MASTER_RPC_ADDRESSES, "localhost:19998");
        ServerConfiguration.set(PropertyKey.JOB_MASTER_RPC_PORT, "20001");

        // Retrieve master RPC addresses from the configuration API
        List<InetSocketAddress> masterRpcAddresses = ConfigurationUtils.getMasterRpcAddresses(ServerConfiguration.global());

        // Retrieve the job master RPC port through the configuration API
        int jobRpcPort = ServerConfiguration.global().getInt(PropertyKey.JOB_MASTER_RPC_PORT);

        // Invoke the target method for testing
        List<InetSocketAddress> jobMasterRpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(ServerConfiguration.global());

        // Validate fallback behavior: Ensure returned addresses match hostnames from master RPC addresses 
        // but have the job master RPC port
        Assert.assertEquals(masterRpcAddresses.size(), jobMasterRpcAddresses.size());
        for (int i = 0; i < masterRpcAddresses.size(); i++) {
            Assert.assertEquals(masterRpcAddresses.get(i).getHostName(), jobMasterRpcAddresses.get(i).getHostName());
            Assert.assertEquals(jobRpcPort, jobMasterRpcAddresses.get(i).getPort());
        }
    }
}