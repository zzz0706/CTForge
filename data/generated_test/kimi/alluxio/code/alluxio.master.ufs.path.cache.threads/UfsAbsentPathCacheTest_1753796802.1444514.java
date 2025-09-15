package alluxio.master.file.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.NoopUfsAbsentPathCache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertTrue;

@PrepareForTest({ServerConfiguration.class})
public class UfsAbsentPathCacheTest {

  @Test
  public void testCreateReturnsNoopCacheWhenThreadsNegative() {
    // 1. Prepare the test conditions
    ServerConfiguration.set(PropertyKey.MASTER_UFS_PATH_CACHE_THREADS, -10);

    // 2. Invoke the method under test
    UfsAbsentPathCache result = UfsAbsentPathCache.Factory.create(null);

    // 3. Assertions
    assertTrue("Expected NoopUfsAbsentPathCache when threads <= 0",
        result instanceof NoopUfsAbsentPathCache);
  }
}