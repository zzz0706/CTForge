package alluxio.conf;

import static org.junit.Assert.assertEquals;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

public class CustomTierPresentInOrderPassesValidationTest {

  @Test
  public void customTierPresentInOrderPassesValidation() {
    // 1. Instantiate configuration using Alluxio 2.1.0 API
    InstancedConfiguration conf =
        new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare test conditions: set custom tier and its value
    conf.set(PropertyKey.LOCALITY_ORDER, "node,rack,dc");
    conf.set(PropertyKey.Template.LOCALITY_TIER.format("dc"), "us-west");

    // 3. Invoke validation (method under test)
    conf.validate();

    // 4. No exception thrown implies validation passes; verify expected state
    assertEquals("node,rack,dc", conf.get(PropertyKey.LOCALITY_ORDER));
    assertEquals("us-west", conf.get(PropertyKey.Template.LOCALITY_TIER.format("dc")));
  }
}