package alluxio.master.file;

import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlockIntegrityCheckIntervalValidationTest {

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void testBlockIntegrityCheckIntervalValid() {
    // 1. Configuration is read from ServerConfiguration, no explicit setting in test
    // 2. Default value is 1hr (3600000 ms) – any value <=0 disables the check
    // 3. Ensure the value is an integer (ms) and >= -1 (<=0 disables)
    long interval = ServerConfiguration.getMs(PropertyKey.MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_INTERVAL);
    assertTrue("Block integrity check interval must be an integer (ms)",
        interval >= -1);
  }
}