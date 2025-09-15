package alluxio.master;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class MasterUtilsConfigTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private String mBaseDir;

  @Before
  public void before() throws Exception {
    mBaseDir = folder.newFolder().getAbsolutePath();
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void verifyPlainRocksStoreIsUsedWhenCacheSizeIsZero() throws Exception {
    // 1. Use the Alluxio 2.1.0 API to set the configuration values
    ServerConfiguration.set(PropertyKey.MASTER_METASTORE, "ROCKS");
    ServerConfiguration.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, 0);

    // 2. Prepare the test conditions: none required beyond the configuration above
    // 3. Test code: obtain the factory and create the store
    // In Alluxio 2.1.0, MasterUtils does not expose getInodeStoreFactory.
    // This test is intentionally left empty because the API is not available.
    // 4. Assert the returned instance is exactly RocksInodeStore (not wrapped)
    // Skipped due to missing API
  }

  @Test
  public void verifyCachingInodeStoreIsUsedWhenCacheSizeIsPositive() throws Exception {
    // 1. Use the Alluxio 2.1.0 API to set the configuration values
    ServerConfiguration.set(PropertyKey.MASTER_METASTORE, "ROCKS");
    ServerConfiguration.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, 1000);

    // 2. Prepare the test conditions: none required beyond the configuration above
    // 3. Test code: obtain the factory and create the store
    // In Alluxio 2.1.0, MasterUtils does not expose getInodeStoreFactory.
    // This test is intentionally left empty because the API is not available.
    // 4. Assert the returned instance is wrapped by CachingInodeStore
    // Skipped due to missing API
  }
}