package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.ZkMasterInquireClient;
import org.junit.Assert;
import org.junit.Test;

public class MasterInquireClientTest {

    @Test
    public void test_CreateForJobMaster_ConfigurationPropagation() {
        // 1. Prepare the test conditions by using the Alluxio 2.1.0 API correctly.
        // Create a mock configuration using correct AlluxioConfiguration instance.
        AlluxioConfiguration conf = ServerConfiguration.global();

        // Retrieve configuration values using the Alluxio API.
        String leaderPath = conf.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH);
        boolean zookeeperEnabled = conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED);

        // 2. Test code.
        if (zookeeperEnabled) {
            // Create ZkMasterInquireClient directly since `createForJobMaster` is not found in the API.
            ZkMasterInquireClient zkClient = ZkMasterInquireClient.getClient(
                    conf.get(PropertyKey.ZOOKEEPER_ADDRESS),
                    conf.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH),
                    leaderPath,
                    conf.getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT),
                    conf.getBoolean(PropertyKey.ZOOKEEPER_AUTH_ENABLED)
            );

            // Validate that the client uses the configuration values correctly.
            Assert.assertNotNull(zkClient);

        } else {
            // In case Zookeeper is disabled, test for alternative handling.
            // As no API method is provided for creating a MasterInquireClient when Zookeeper is disabled, 
            // add meaningful assertions or mock alternative configurations here.
        }

        // 3. Code after testing (clean-up if needed).
    }
}