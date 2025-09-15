package alluxio.conf;

import org.junit.Test;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ConfigurationValidationTest {

    @Test
    public void testZookeeperJobLeaderPathConfiguration() {
        /*
         * Step 1: Based on the understood constraints and dependencies, determine whether the read configuration value satisfies the constraints and dependencies.
         * Step 2: Verify whether the value of this configuration item satisfies the constraints and dependencies.
         *         For `alluxio.zookeeper.job.leader.path`, appropriately validate:
         *         1. Configuration value should not be null or empty.
         *         2. Configuration value should represent a valid path (non-empty, valid format).
         */

        // Step 1: Obtain the current configuration value using the Alluxio API
        AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
        String leaderPath = conf.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH);

        // Step 2: Validate that the configuration value is not null or empty
        assertFalse("Zookeeper job leader path should not be null or empty", leaderPath == null || leaderPath.isEmpty());

        // Step 3: Validate that the configuration value represents a valid path
        assertTrue("Zookeeper job leader path should start with '/'", leaderPath.startsWith("/"));
        assertFalse("Zookeeper job leader path should not end with '/'", leaderPath.endsWith("//"));
    }
}