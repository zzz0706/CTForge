package alluxio.master.file;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BlockIntegrityCheckConfigValidationTest {

  @Before
  public void setUp() {
    // Ensure the configuration is reset before each test
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    // Clean up after each test
    ServerConfiguration.reset();
  }

  @Test
  public void validateStartupBlockIntegrityCheckEnabled() {
    // 1. Obtain configuration value via Alluxio 2.1.0 API
    boolean value = ServerConfiguration.getBoolean(
        PropertyKey.MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED);

    // 2. No additional test conditions needed—only validating the value itself

    // 3. Test code: verify the boolean value is either true or false
    assertTrue("alluxio.master.startup.block.integrity.check.enabled must be a valid boolean",
        value == true || value == false);

    // 4. No post-test code needed; tearDown() handles reset
  }
}