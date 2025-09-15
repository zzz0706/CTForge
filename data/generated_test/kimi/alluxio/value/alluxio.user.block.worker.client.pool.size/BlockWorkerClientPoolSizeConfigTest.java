package alluxio.client.file;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BlockWorkerClientPoolSizeConfigTest {

  @Test
  public void validateBlockWorkerClientPoolSize() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 2. Prepare the test conditions.
    int poolSize = conf.getInt(alluxio.conf.PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE);

    // 3. Test code.
    assertTrue("Block worker client pool size must be a positive integer",
               poolSize > 0);

    // 4. Code after testing.
  }
}