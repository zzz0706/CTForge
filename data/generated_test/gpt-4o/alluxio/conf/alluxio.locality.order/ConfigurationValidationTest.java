package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class ConfigurationValidationTest {

  /**
   * Test to validate whether the configuration `alluxio.locality.order` satisfies the constraints 
   * and dependencies outlined in the source code.
   */
  @Test
  public void testValidLocalityOrderConfiguration() {
    // Step 1: Obtain the configuration instance
    InstancedConfiguration conf = InstancedConfiguration.defaults();

    // Step 2: Read the value of "alluxio.locality.order" from the configuration
    List<String> localityOrder = conf.getList(PropertyKey.LOCALITY_ORDER, ",");

    // Step 3: Check if the locality order is not empty
    Assert.assertFalse("The `alluxio.locality.order` configuration must not be empty.", localityOrder.isEmpty());

    // Step 4: Validate each tier in the locality order
    Set<String> definedTiers = new HashSet<>(localityOrder);
    Set<PropertyKey> predefinedKeys = new HashSet<>(PropertyKey.defaultKeys());

    for (PropertyKey key : conf.keySet()) {
      if (predefinedKeys.contains(key)) {
        // Skip non-templated keys
        continue;
      }

      // Match keys to custom locality tiers
      String tierRegex = "^alluxio\\.locality\\.(.+?)$";
      String keyName = key.getName();
      boolean isTierKey = keyName.matches(tierRegex);

      if (isTierKey) {
        // Extract the tier name from the key (e.g., `alluxio.locality.custom` → `custom`)
        String tierName = keyName.replaceFirst(tierRegex, "$1");

        // Check if the custom tier exists in the tier list defined by `alluxio.locality.order`
        Assert.assertTrue(String.format(
            "Tier '%s' is configured by '%s', but it does not exist in the tier list %s "
                + "configured by `alluxio.locality.order`.",
            tierName, keyName, definedTiers),
            definedTiers.contains(tierName));
      }
    }
  }

  /**
   * Test to validate the correct tiered configuration using TieredIdentityFactory methods.
   */
  @Test
  public void testTieredIdentityValidation() {
    // Step 1: Obtain the configuration instance
    InstancedConfiguration conf = InstancedConfiguration.defaults();

    // Step 2: Get the `alluxio.locality.order` configuration
    List<String> localityOrder = conf.getList(PropertyKey.LOCALITY_ORDER, ",");
    Set<String> definedTiers = new HashSet<>(localityOrder);

    // Step 3: Simulate a tiered identity string and validate consistency
    String identityString = "node=node1,rack=rack1";
    String[] tierEntries = identityString.split(",");

    for (String entry : tierEntries) {
      String[] parts = entry.split("=");
      Assert.assertEquals("Each tier entry should be in 'key=value' format.", 2, parts.length);

      String tierName = parts[0].trim();
      String tierValue = parts[1].trim();

      // Validate that the tier entry corresponds to a known tier
      Assert.assertTrue(String.format(
          "Unrecognized tier: %s. The tiers defined by `alluxio.locality.order` are %s.",
          tierName, definedTiers),
          definedTiers.contains(tierName));

      // Additional checks can be implemented here for tier values if required
      Assert.assertFalse("Tier values must not be empty.", tierValue.isEmpty());
    }
  }
}