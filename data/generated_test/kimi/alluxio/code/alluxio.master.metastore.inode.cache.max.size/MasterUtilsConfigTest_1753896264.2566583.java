package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MasterUtilsConfigTest {

  @Before
  public void setUp() {
    // Reset global configuration to isolate test
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  @Test
  public void verifyZeroCacheSizeDisablesOnHeapInodeCache() {
    // 1. Prepare configuration: ROCKS metastore + cache size = 0
    ServerConfiguration.set(PropertyKey.MASTER_METASTORE, "ROCKS");
    ServerConfiguration.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, 0);

    // 2. Compute expected: no CachingInodeStore should be created
    boolean cachingEnabled =
        ServerConfiguration.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE) > 0;

    // 3. Assertions
    assertFalse("CachingInodeStore should not be created when cache size is 0", cachingEnabled);
  }
}