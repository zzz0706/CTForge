package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterInquireClient;
import alluxio.master.SingleMasterInquireClient;
import alluxio.util.ConfigurationUtils;
import alluxio.exception.status.UnavailableException;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class MasterInquireClientTest {

    @Test
    public void testCreateForJobMasterWithZKDisabled() throws UnavailableException {
        // 1. Initialize an AlluxioConfiguration object and set PropertyKey.ZOOKEEPER_ENABLED to false.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        conf.set(PropertyKey.ZOOKEEPER_ENABLED, "false");

        // 2. Populate configuration values used for fallback mechanisms like job master addresses.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);

        // Check that fallback is correctly populated
        Assert.assertNotNull("The list of job master RPC addresses should not be null.", addresses);
        Assert.assertFalse("The list of job master RPC addresses should not be empty.", addresses.isEmpty());

        // 3. Create a MasterInquireClient to handle the ZK-disabled state.
        InetSocketAddress primaryAddress = addresses.get(0);
        MasterInquireClient client = new SingleMasterInquireClient(primaryAddress);

        // 4. Validate that the client was initialized correctly.
        InetSocketAddress clientAddress = client.getPrimaryRpcAddress();
        Assert.assertNotNull("Client address should not be null.", clientAddress);
        Assert.assertEquals("Client address does not match expected primary address.", primaryAddress, clientAddress);
    }
}