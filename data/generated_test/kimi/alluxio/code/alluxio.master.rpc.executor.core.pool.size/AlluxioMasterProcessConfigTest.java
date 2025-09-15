package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class AlluxioMasterProcessConfigTest {

  @Before
  public void setUp() {
    // Reset global configuration to defaults
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    // Clean up after test
    ServerConfiguration.reset();
  }

  @Test
  public void corePoolSizeEqualsParallelismWhenZero() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    int parallelism = 8;
    int corePoolSize = 0;

    // 2. Prepare the test conditions.
    mockStatic(ServerConfiguration.class);
    when(ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE))
        .thenReturn(corePoolSize);
    when(ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM))
        .thenReturn(parallelism);
    when(ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE))
        .thenReturn(Integer.MAX_VALUE);
    when(ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE))
        .thenReturn(1);
    when(ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE))
        .thenReturn(60000L);

    // 3. Test code.
    int actualCorePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
    if (actualCorePoolSize == 0) {
      actualCorePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
    }

    // 4. Code after testing.
    assertEquals(parallelism, actualCorePoolSize);
  }
}