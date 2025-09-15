package alluxio.master;

import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;

import org.junit.Test;

public class AlluxioMasterRpcExecutorCorePoolSizeValidationTest {

  @Test
  public void validateCorePoolSizeConfiguration() {
    // 1. Use the Alluxio2.1.0 API to obtain configuration values.
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 2. Prepare the test conditions.
    int corePoolSize = conf.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
    int parallelism  = conf.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
    int maxPoolSize  = conf.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);

    // 3. Test code – validate constraints derived from source.
    //    ForkJoinPool enforces:
    //      - corePoolSize must be >= 0
    //      - corePoolSize must not exceed MAX_CAP (0x7fff)
    //      - corePoolSize must not exceed maxPoolSize (enforced in ForkJoinPool ctor)
    assertTrue("corePoolSize must be non-negative", corePoolSize >= 0);
    assertTrue("corePoolSize must not exceed MAX_CAP (32767)",
               corePoolSize <= 0x7fff);
    assertTrue("corePoolSize must not exceed maxPoolSize",
               corePoolSize <= maxPoolSize);

    // 4. Code after testing – nothing to clean up.
  }
}