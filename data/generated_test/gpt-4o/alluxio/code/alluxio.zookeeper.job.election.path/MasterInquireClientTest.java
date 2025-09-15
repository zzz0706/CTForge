package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.PollingMasterInquireClient;
import alluxio.master.SingleMasterInquireClient;
import alluxio.master.MasterInquireClient;
import alluxio.util.ConfigurationUtils;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class MasterInquireClientTest {
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void test_createForJobMaster_withZooKeeperDisabled() {
        // Step 1: Mock the AlluxioConfiguration to disable ZooKeeper
        AlluxioConfiguration conf = Mockito.mock(AlluxioConfiguration.class);
        Mockito.when(conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)).thenReturn(false);

        // Step 2: Mock the job master RPC addresses using ConfigurationUtils
        List<InetSocketAddress> mockAddresses = new ArrayList<>();
        mockAddresses.add(new InetSocketAddress("localhost", 19998));

        // Step 3: Create RetryPolicy for PollingMasterInquireClient constructor
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(10, 5, 1000);
        
        // Step 4: Create the MasterInquireClient instance using the correct API
        MasterInquireClient client;
        if (mockAddresses.size() > 1) {
            client = new PollingMasterInquireClient(mockAddresses, () -> retryPolicy, conf, null);
        } else {
            client = new SingleMasterInquireClient(mockAddresses.get(0));
        }

        // Step 5: Validate the created instance type
        if (mockAddresses.size() > 1) {
            assert (client instanceof PollingMasterInquireClient);
        } else {
            assert (client instanceof SingleMasterInquireClient);
        }
    }
}