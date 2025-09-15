package alluxio.conf;   

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {
    @Test
    public void testMasterRpcExecutorKeepAliveConfigurationValidation() {
        // Step 1: Set up the configuration instance using InstancedConfiguration
        AlluxioProperties props = new AlluxioProperties();
        InstancedConfiguration conf = new InstancedConfiguration(props);

        // Step 2: Obtain and validate the MASTER_RPC_EXECUTOR_KEEPALIVE configuration value
        String keepAliveValue = conf.get(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        Assert.assertNotNull("Configuration value for MASTER_RPC_EXECUTOR_KEEPALIVE is null", keepAliveValue);
        Assert.assertFalse("Configuration value for MASTER_RPC_EXECUTOR_KEEPALIVE is empty", keepAliveValue.isEmpty());

        try {
            long keepAliveMs = conf.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
            Assert.assertTrue("Configuration value for MASTER_RPC_EXECUTOR_KEEPALIVE must be greater than 0", keepAliveMs > 0);
        } catch (NumberFormatException e) {
            Assert.fail("Configuration value for MASTER_RPC_EXECUTOR_KEEPALIVE is not a valid duration");
        }

        // Step 3: Validate dependent configuration properties
        try {
            int parallelism = conf.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
            int maxPoolSize = conf.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);

            Assert.assertTrue("Parallelism must be less than or equal to Max Pool Size", parallelism <= maxPoolSize);
            Assert.assertTrue("Parallelism must be greater than zero", parallelism > 0);
            Assert.assertTrue("Max Pool Size must be greater than zero", maxPoolSize > 0);
        } catch (Exception e) {
            Assert.fail("Failed to validate configuration dependencies for MASTER_RPC_EXECUTOR_KEEPALIVE");
        }
    }
}