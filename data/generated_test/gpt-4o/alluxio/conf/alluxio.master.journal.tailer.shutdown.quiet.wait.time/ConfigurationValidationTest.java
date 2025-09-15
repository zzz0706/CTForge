package alluxio.master.journal;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {

  @Test
  public void testJournalTailerShutdownQuietWaitTimeConfiguration() {
    /*
     * Step 1: Based on the understood constraints and dependencies, determine whether the read configuration value satisfies the constraints and dependencies.
     * Step 2: Verify whether the value of this configuration item satisfies the constraints and dependencies.
     * 
     * Constraints:
     * - The configuration value must be a valid time format.
     * - The configuration value should not be negative.
     */

    // Step 1: Retrieve the configuration value using the Alluxio 2.1.0 API.
    String quietWaitTimeStr = ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);

    // Step 2: Ensure the configuration value is not null or empty.
    Assert.assertNotNull("Configuration value cannot be null", quietWaitTimeStr);
    Assert.assertFalse("Configuration value cannot be empty", quietWaitTimeStr.isEmpty());

    // Step 3: Parse the value and ensure it's valid.
    try {
      long quietWaitTimeMs = ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);

      // Verify that the value is non-negative.
      Assert.assertTrue("Configuration value must be non-negative", quietWaitTimeMs >= 0);

      // Optionally log the value for debugging purposes.
      System.out.println("Quiet wait time (ms): " + quietWaitTimeMs);

    } catch (NumberFormatException e) {
      Assert.fail("Configuration value must be a valid time format (e.g., '5sec', '100ms')");
    }
  }
}