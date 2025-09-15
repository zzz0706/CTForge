package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConfigurationUtilsTest {

    @Test
    public void testGetJobMasterRpcAddressesFallbackToMasterAddresses() {
        // 1. Prepare the test conditions: initialize AlluxioConfiguration and set required values.
        InstancedConfiguration conf = new InstancedConfiguration(
                ConfigurationUtils.defaults());

        // Simulate getting ports dynamically via the API (not hardcoding values).
        int jobRpcPort = NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RPC, conf);

        // Simulate getting master RPC addresses dynamically via the API (not hardcoding values).
        List<InetSocketAddress> masterRpcAddresses = ConfigurationUtils.getMasterRpcAddresses(conf);

        // 2. Test code: call the target method and test fallback behavior.
        List<InetSocketAddress> jobMasterRpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // 3. Code after testing: validate the results.
        // Ensure that the 'jobMasterRpcAddresses' reuses the hosts from 'masterRpcAddresses' but overrides the port.
        assertEquals(masterRpcAddresses.size(), jobMasterRpcAddresses.size());
        for (int i = 0; i < masterRpcAddresses.size(); i++) {
            InetSocketAddress masterAddress = masterRpcAddresses.get(i);
            InetSocketAddress jobMasterAddress = jobMasterRpcAddresses.get(i);

            // Validate that the hostnames are identical.
            assertEquals(masterAddress.getHostName(), jobMasterAddress.getHostName());
            // Validate that the ports are overridden with the Job Master RPC port.
            assertEquals(jobRpcPort, jobMasterAddress.getPort());
        }
    }
}