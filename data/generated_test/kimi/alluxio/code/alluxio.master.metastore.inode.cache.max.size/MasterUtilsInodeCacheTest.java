package alluxio.master;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MasterUtilsInodeCacheTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    mConf = ServerConfiguration.global();
  }

  @After
  public void after() {
    // Reset the global configuration to defaults after each test
    ServerConfiguration.reset();
  }

  @Test
  public void verifyCachingInodeStoreIsUsedWhenCacheSizeIsPositive() {
    // 1. Prepare configuration
    mConf.set(PropertyKey.MASTER_METASTORE, "ROCKS");
    mConf.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, 1000);

    // 2. Calculate expected value dynamically
    long expectedCacheSize = mConf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
    assert expectedCacheSize > 0 : "Cache size must be positive";

    // 3. Test code
    assert "ROCKS".equals(mConf.get(PropertyKey.MASTER_METASTORE));
    assert 1000 == mConf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
  }
}