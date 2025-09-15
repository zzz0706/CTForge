package alluxio.master.file.meta;

import static org.junit.Assert.assertEquals;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;

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
}