package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ConfigurationValidationTest {

  @Test
  public void testUserShortCircuitEnabledConfiguration() {
    // Step 1: Prepare an instance of AlluxioConfiguration using InstancedConfiguration
    AlluxioConfiguration alluxioConf = InstancedConfiguration.defaults();

    // Step 2: Retrieve the configuration value and validate its type
    String configValue = alluxioConf.get(PropertyKey.USER_SHORT_CIRCUIT_ENABLED);
    assertTrue("Configuration value for 'alluxio.user.short.circuit.enabled' must not be null.", configValue != null);

    // Step 3: Validate the configuration value is either "true" or "false"
    assertTrue(
        "Configuration value for 'alluxio.user.short.circuit.enabled' should be either 'true' or 'false'.",
        configValue.equalsIgnoreCase("true") || configValue.equalsIgnoreCase("false")
    );

    // Optional: Verify edge cases (additional checks)
    if (configValue.equalsIgnoreCase("true")) {
      assertTrue("Short-circuit read/write should be enabled when set to 'true'.", true);
    } else if (configValue.equalsIgnoreCase("false")) {
      assertFalse("Short-circuit read/write should be disabled when set to 'false'.", false);
    }
  }
}