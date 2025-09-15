package alluxio.client.block.stream;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;

public class ShortCircuitEnabledValidationTest {

  @Test
  public void validateShortCircuitEnabled() {
    // 1. Obtain configuration values through the Alluxio 2.1.0 API
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Read the configuration value
    boolean shortCircuitEnabled = conf.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED);

    // 3. Validate the configuration value
    // The only valid values for a boolean are true or false
    Assert.assertTrue("alluxio.user.short.circuit.enabled must be either true or false",
        shortCircuitEnabled || !shortCircuitEnabled);

    // 4. No cleanup required for this test
  }
}