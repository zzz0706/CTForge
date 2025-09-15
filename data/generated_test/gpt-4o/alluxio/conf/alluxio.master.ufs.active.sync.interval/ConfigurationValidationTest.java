package alluxio.master.file;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;
import org.junit.Assert;

public class ConfigurationValidationTest {

  /**
   * Unit test to validate the constraints and dependencies of the configuration
   * `alluxio.master.ufs.active.sync.interval`.
   */
  @Test
  public void testMasterUfsActiveSyncIntervalConfigValidity() {
    // Step 1: Read the value of the configuration using Alluxio 2.1.0 API.
    String syncIntervalStr = ServerConfiguration.get(PropertyKey.MASTER_UFS_ACTIVE_SYNC_INTERVAL);

    // Step 2: Assert that the configuration is not null or empty.
    Assert.assertNotNull(
        "Configuration alluxio.master.ufs.active.sync.interval should not be null.", syncIntervalStr);
    Assert.assertFalse(
        "Configuration alluxio.master.ufs.active.sync.interval should not be empty.", syncIntervalStr.isEmpty());

    // Step 3: Validate the format of the value.
    // The expected format is a time duration, such as "30sec", "1min", etc.
    Assert.assertTrue("Configuration alluxio.master.ufs.active.sync.interval has an invalid format.",
        syncIntervalStr.matches("\\d+(sec|min|ms|hr)"));

    // Step 4: Validate if the value is logically acceptable.
    // Convert the value to milliseconds for further validation.
    long syncIntervalMs = ServerConfiguration.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_INTERVAL);
    Assert.assertTrue(
        "Configuration alluxio.master.ufs.active.sync.interval must be greater than zero.",
        syncIntervalMs > 0);

    // No additional dependencies are defined for this configuration in the provided source code snippet.
    // The test assumes no extra constraints or dependencies exist based on the given information.
  }
}