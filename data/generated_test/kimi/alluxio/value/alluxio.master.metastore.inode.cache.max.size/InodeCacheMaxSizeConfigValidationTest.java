package alluxio.master.metastore;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class InodeCacheMaxSizeConfigValidationTest {

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void testInodeCacheMaxSizeNonNegative() {
    InstancedConfiguration conf = ServerConfiguration.global();
    int maxSize = conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
    Assert.assertTrue(
        PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE.getName()
            + " must be non-negative, but was " + maxSize,
        maxSize >= 0);
  }

  @Test
  public void testInodeCacheMaxSizePositiveWhenRocksAndCaching() {
    InstancedConfiguration conf = ServerConfiguration.global();
    String metastoreType = conf.get(PropertyKey.MASTER_METASTORE);
    int maxSize = conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);

    if ("ROCKS".equalsIgnoreCase(metastoreType) && maxSize != 0) {
      Assert.assertTrue(
          PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE.getName()
              + " must be positive when using ROCKS metastore with on-heap caching enabled, but was "
              + maxSize,
          maxSize > 0);
    }
  }
}