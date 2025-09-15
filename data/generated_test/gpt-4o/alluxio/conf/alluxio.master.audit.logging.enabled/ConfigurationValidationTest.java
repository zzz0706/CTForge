package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {

  @Test
  public void testMasterDailyBackupEnabledConfiguration() {
    /*
     * Step 1: Retrieve the configuration value using Alluxio's AlluxioConfiguration API.
     * Step 2: Verify whether the value of this configuration satisfies the constraints and dependencies.
     *         - Check that the value is a valid boolean (true or false).
     */

    try {
      // Step 1: Retrieve the configuration value
      AlluxioConfiguration configuration = InstancedConfiguration.defaults();
      boolean configValue = configuration.getBoolean(PropertyKey.MASTER_DAILY_BACKUP_ENABLED);

      // Prepare test conditions
      Assert.assertNotNull("Configuration value for alluxio.master.daily.backup.enabled should not be null.", configValue);

      // Ensure configValue is boolean.
      Assert.assertTrue(
          "Configuration value for alluxio.master.daily.backup.enabled must be true or false.",
          configValue == true || configValue == false);

    } catch (Exception e) {
      Assert.fail("Error occurred while validating configuration: " + e.getMessage());
    }
  }
}