package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.resource.ResourcePool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

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
}