package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.MountPOptions;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class UfsAbsentPathCacheTest {

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void testCreateReturnsNoopCacheWhenThreadsZero() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions: set configuration value to 0
    ServerConfiguration.set(PropertyKey.MASTER_UFS_PATH_CACHE_THREADS, 0);

    // 3. Prepare required constructor arguments for MountTable
    UfsManager ufsManager = new UfsManager() {
      @Override
      public UfsManager.UfsClient get(long mountId) {
        return null;
      }

      @Override
      public void addMount(long mountId, AlluxioURI ufsUri,
                           UnderFileSystemConfiguration ufsConf) {
      }

      @Override
      public void removeMount(long mountId) {
      }

      @Override
      public void close() {
      }

      @Override
      public UfsManager.UfsClient getRoot() {
        return null;
      }
    };
    MountTable mountTable = new MountTable(ufsManager, new MountInfo(
        new AlluxioURI("/"),
        new AlluxioURI("/tmp"),
        1,
        MountPOptions.getDefaultInstance()
    ));

    // 4. Invoke the method under test
    UfsAbsentPathCache cache = UfsAbsentPathCache.Factory.create(mountTable);

    // Code after testing
    assertTrue(cache instanceof NoopUfsAbsentPathCache);
  }
}