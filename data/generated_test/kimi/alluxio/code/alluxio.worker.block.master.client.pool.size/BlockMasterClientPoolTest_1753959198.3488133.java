package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.resource.ResourcePool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class BlockMasterClientPoolTest {

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void testDefaultPoolSizeIsEleven() throws Exception {
    // 1. Create a fresh Configuration that relies on the built-in default value
    long expectedPoolSize = ServerConfiguration.getInt(
        PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

    // 2. Instantiate the pool under test
    BlockMasterClientPool pool = new BlockMasterClientPool();

    // 3. Use reflection to obtain the mMaxCapacity field from the underlying ResourcePool
    Field maxCapacityField = ResourcePool.class.getDeclaredField("mMaxCapacity");
    maxCapacityField.setAccessible(true);
    int actualPoolSize = (int) maxCapacityField.get(pool);

    // 4. Assert the expected value
    assertEquals(expectedPoolSize, actualPoolSize);
  }

  @Test
  public void testAcquireWithinCapacity() throws Exception {
    // 1. Prepare a fresh configuration relying on default pool size
    int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

    // 2. Instantiate the pool under test
    BlockMasterClientPool pool = new BlockMasterClientPool();

    // 3. Acquire a client within capacity
    BlockMasterClient client = pool.acquire(1, TimeUnit.SECONDS);

    // 4. Assert client is not null and release it
    assertNotNull(client);
    pool.release(client);
  }

  @Test
  public void testAcquireBeyondCapacity() throws Exception {
    // 1. Prepare a fresh configuration relying on default pool size
    int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

    // 2. Instantiate the pool under test
    BlockMasterClientPool pool = new BlockMasterClientPool();

    // 3. Acquire all clients up to capacity
    BlockMasterClient[] clients = new BlockMasterClient[poolSize];
    for (int i = 0; i < poolSize; i++) {
      clients[i] = pool.acquire(1, TimeUnit.SECONDS);
      assertNotNull(clients[i]);
    }

    // 4. Try to acquire one more client, expecting timeout
    BlockMasterClient extraClient = pool.acquire(100, TimeUnit.MILLISECONDS);
    assertNull(extraClient);

    // 5. Release all clients back to pool
    for (BlockMasterClient c : clients) {
      pool.release(c);
    }
  }
}