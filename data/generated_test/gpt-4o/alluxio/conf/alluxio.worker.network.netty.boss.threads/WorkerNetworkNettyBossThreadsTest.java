package alluxio.conf;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class WorkerNetworkNettyBossThreadsTest {
    
    @Test
    public void testWorkerNetworkNettyBossThreadsConfig() {
        // Prepare the AlluxioProperties object required for InstancedConfiguration
        AlluxioProperties properties = new AlluxioProperties();
        
        // Create an instance of InstancedConfiguration with the AlluxioProperties
        InstancedConfiguration conf = new InstancedConfiguration(properties);

        // Attempt to fetch the configuration value for "alluxio.worker.network.netty.boss.threads"
        try {
            // Fetch configuration value using the Alluxio Configuration API
            int bossThreads = conf.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);

            // Constraint 1: Ensure the value is an integer and greater than or equal to 1
            Assert.assertTrue("The value for `alluxio.worker.network.netty.boss.threads` must be >= 1", bossThreads >= 1);

        } catch (Exception e) {
            // Fail the test if any exception occurs while validating the configuration
            Assert.fail("Failed to validate the configuration `alluxio.worker.network.netty.boss.threads`: " + e.getMessage());
        }
    }
}