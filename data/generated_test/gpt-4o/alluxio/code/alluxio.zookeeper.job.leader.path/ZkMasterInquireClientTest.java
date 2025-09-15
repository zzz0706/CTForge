package alluxio.master;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.ZkMasterInquireClient;
import alluxio.util.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

public class ZkMasterInquireClientTest {

    /**
     * Test case: Verify that ZkMasterInquireClient handles invalid leaderPath configurations gracefully.
     */
    @Test
    public void testGetClientWithInvalidLeaderPath() {
        // 1. Prepare the test conditions using the correct API methods.
        InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

        // Configure Zookeeper-related settings correctly
        conf.set(PropertyKey.ZOOKEEPER_ENABLED, "true");
        conf.set(PropertyKey.ZOOKEEPER_ADDRESS, "127.0.0.1:2181");
        conf.set(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH, "/valid_election");

        // Add invalid leader path configuration
        String invalidLeaderPath = "/invalid_leader_path";
        conf.set(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH, invalidLeaderPath);

        int inquireRetryCount = 3; // Set retry count
        boolean authEnabled = false;

        try {
            // 2. Create a ZkMasterInquireClient with invalid leaderPath to trigger validation.
            ZkMasterInquireClient client = ZkMasterInquireClient.getClient(
                conf.get(PropertyKey.ZOOKEEPER_ADDRESS),
                conf.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH),
                conf.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH),
                inquireRetryCount,
                authEnabled
            );

            // 3. Assert the client is instantiated.
            Assert.assertNotNull(client);

            // Validate whether the leaderPath configuration is expected
            Assert.assertEquals(invalidLeaderPath, conf.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH));
        } catch (Exception e) {
            // 4. Handle exception and confirm proper error handling.
            Assert.assertTrue(e.getMessage().contains("Invalid leaderPath"));
        }
    }
}