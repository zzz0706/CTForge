package alluxio.conf; 

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.ConfigurationTestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {
    // Test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void testMasterRpcExecutorCorePoolSize() {
        // Step 1: Obtain the configuration value using InstancedConfiguration
        AlluxioConfiguration configuration = ConfigurationTestUtils.defaults();
        int corePoolSize = configuration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);

        // Step 2: Validate the configuration against constraints
        // Validate that the core pool size value is >= 0
        Assert.assertTrue("The value of 'alluxio.master.rpc.executor.core.pool.size' must be >= 0", corePoolSize >= 0);
        Assert.assertEquals(16, corePoolSize);
        // Ensure it does not exceed other related constraints if dependencies are determined
        int parallelism = configuration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        Assert.assertTrue("The value of 'alluxio.master.rpc.executor.core.pool.size' must not exceed parallelism",
                corePoolSize <= parallelism);
    }
}