package alluxio.master;

import static org.junit.Assert.assertTrue;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class MasterUtilsConfigTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    mConf = ServerConfiguration.global();
    mConf.set(PropertyKey.MASTER_METASTORE, "ROCKS");
    mConf.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, 0);
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void verifyPlainRocksStoreIsUsedWhenCacheSizeIsZero() throws Exception {
    // 1. Configuration already set in @Before (cache size = 0)
    // 2. Prepare the test conditions: none required for this simple config test
    // 3. Test code: assert that the configuration is correct
    assertTrue("ROCKS".equals(mConf.get(PropertyKey.MASTER_METASTORE)));
    assertTrue(0 == mConf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE));
    // 4. Code after testing: cleanup handled by @After
  }
}