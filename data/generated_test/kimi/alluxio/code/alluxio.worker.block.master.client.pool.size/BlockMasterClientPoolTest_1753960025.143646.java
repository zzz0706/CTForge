package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertNull;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class})
public class BlockMasterClientPoolTest {

  @Test
  public void testZeroPoolSizeDisablesCreation() throws Exception {
    // 1. Obtain configuration values via ServerConfiguration
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE, 0);

    // 2. Prepare the test conditions: create pool under zero-size configuration
    BlockMasterClientPool pool = new BlockMasterClientPool();

    // 3. Test code: attempt to acquire a client
    BlockMasterClient client = pool.acquire(100, java.util.concurrent.TimeUnit.MILLISECONDS);

    // 4. Code after testing: assert null because pool size is zero
    assertNull("acquire should return null when pool size is 0", client);
  }
}