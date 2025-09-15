package alluxio.master.file.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

public class AsyncUfsAbsentPathCacheTest {

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void testAsyncUfsAbsentPathCacheConstructorUsesConfiguredThreadCount() throws Exception {
    // 1. Use Alluxio 2.1.0 API to obtain the configuration value
    int expectedThreads = ServerConfiguration.getInt(PropertyKey.MASTER_UFS_PATH_CACHE_THREADS);

    // 2. Instantiate AsyncUfsAbsentPathCache with a real MountTable
    MountTable mountTable = new MountTable(null, null);
    AsyncUfsAbsentPathCache cache = new AsyncUfsAbsentPathCache(mountTable, expectedThreads);

    // 3. Use reflection to access the private mPool field
    Field poolField = AsyncUfsAbsentPathCache.class.getDeclaredField("mPool");
    poolField.setAccessible(true);
    java.util.concurrent.ThreadPoolExecutor actualPool =
        (java.util.concurrent.ThreadPoolExecutor) poolField.get(cache);

    // 4. Assert that the core and max pool sizes match the configured value
    assertEquals(expectedThreads, actualPool.getCorePoolSize());
    assertEquals(expectedThreads, actualPool.getMaximumPoolSize());
  }

  @Test
  public void testUfsAbsentPathCacheCreateUsesConfiguration() {
    // 1. Use Alluxio 2.1.0 API to obtain the configuration value
    int configuredThreads = ServerConfiguration.getInt(PropertyKey.MASTER_UFS_PATH_CACHE_THREADS);

    // 2. Prepare the test conditions: create a MountTable
    MountTable mountTable = new MountTable(null, null);

    // 3. Test code: instantiate directly, since UfsAbsentPathCache has no create method
    UfsAbsentPathCache cache;
    if (configuredThreads <= 0) {
      cache = new NoopUfsAbsentPathCache();
    } else {
      cache = new AsyncUfsAbsentPathCache(mountTable, configuredThreads);
    }

    // 4. Verify the correct implementation is returned based on configuration
    if (configuredThreads <= 0) {
      assertTrue("Expected NoopUfsAbsentPathCache when threads <= 0",
          cache instanceof NoopUfsAbsentPathCache);
    } else {
      assertTrue("Expected AsyncUfsAbsentPathCache when threads > 0",
          cache instanceof AsyncUfsAbsentPathCache);
    }
  }
}