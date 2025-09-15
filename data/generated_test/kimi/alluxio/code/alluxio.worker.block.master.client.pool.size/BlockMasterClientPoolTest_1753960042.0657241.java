package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class BlockMasterClientPoolTest {

  @After
  public void tearDown() {
    // Reset the configuration after every test to avoid side-effects
    ServerConfiguration.reset();
  }

  @Test
  public void testZeroPoolSizeDisablesCreation() throws Exception {
    // 1. Use the Alluxio 2.1.0 API to set the configuration value
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE, 0);

    // 2. Prepare the test conditions: instantiate the pool under zero-size configuration
    BlockMasterClientPool pool = new BlockMasterClientPool();

    // 3. Test code: attempt to acquire a client with a 100 ms timeout
    BlockMasterClient client = pool.acquire(100, java.util.concurrent.TimeUnit.MILLISECONDS);

    // 4. Code after testing: assert that no client is returned
    assertNull("acquire should return null when pool size is 0", client);
  }
}