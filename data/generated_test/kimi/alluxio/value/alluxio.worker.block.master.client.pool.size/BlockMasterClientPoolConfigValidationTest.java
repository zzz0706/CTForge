package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BlockMasterClientPoolConfigValidationTest {

  @Before
  public void setUp() {
    // Reset configuration to defaults before each test
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    // Clean up after each test
    ServerConfiguration.reset();
  }

  @Test
  public void validateWorkerBlockMasterClientPoolSize() {
    // 1. Obtain the configuration value via Alluxio 2.1.0 API
    int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

    // 2. Prepare test conditions
    // No additional setup needed since we're validating the existing configuration

    // 3. Test code
    // Validate the configuration value meets constraints
    // Based on usage in BlockMasterClientPool and ResourcePool:
    // - Must be positive integer (ResourcePool uses it as capacity)
    // - No upper limit enforced in code, but practical limits apply
    assertTrue("alluxio.worker.block.master.client.pool.size must be a positive integer",
        poolSize > 0);
  }
}