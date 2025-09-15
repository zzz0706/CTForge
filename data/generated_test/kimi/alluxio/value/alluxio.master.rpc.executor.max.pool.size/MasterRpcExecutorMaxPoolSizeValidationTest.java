package alluxio.master;

import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MasterRpcExecutorMaxPoolSizeValidationTest {

  private static final PropertyKey KEY = PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE;
  private static final PropertyKey PARALLELISM_KEY = PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM;

  @Before
  public void before() {
    // No hard-coded value setting; rely on configuration files or defaults
  }

  @After
  public void after() {
    // Reset to defaults if any runtime changes were made
    ServerConfiguration.reset();
  }

  @Test
  public void maxPoolSizeGreaterThanOrEqualToParallelism() {
    int maxPoolSize = ServerConfiguration.getInt(KEY);
    int parallelism = ServerConfiguration.getInt(PARALLELISM_KEY);

    assertTrue(
        String.format(
            "Invalid configuration: %s (%d) must be >= %s (%d)",
            KEY.getName(), maxPoolSize,
            PARALLELISM_KEY.getName(), parallelism),
        maxPoolSize >= parallelism);
  }

  @Test
  public void maxPoolSizePositive() {
    int maxPoolSize = ServerConfiguration.getInt(KEY);
    assertTrue(
        String.format(
            "Invalid configuration: %s must be positive (found %d)",
            KEY.getName(), maxPoolSize),
        maxPoolSize > 0);
  }
}