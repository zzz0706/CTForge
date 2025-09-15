package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CustomTierMissingFromOrderFailsValidationTest {

  @Test
  public void customTierMissingFromOrderFailsValidation() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare the test conditions: set order to "node,rack" and an extra tier "dc"
    conf.set(PropertyKey.LOCALITY_ORDER, "node,rack");
    conf.set(PropertyKey.Template.LOCALITY_TIER.format("dc"), "us-west");

    // 3. Test code: invoke validation and expect failure
    try {
      conf.validate();
      fail("Expected IllegalStateException due to unlisted tier 'dc'");
    } catch (IllegalStateException e) {
      // 4. Code after testing: verify the exception message contains the expected detail
      assertTrue(e.getMessage().contains("Tier dc is configured by alluxio.locality.dc"));
    }
  }
}