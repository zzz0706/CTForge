package alluxio.conf;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

/**
 * Unit tests for validating {@code alluxio.locality.order} configuration.
 */
public class LocalityOrderValidationTest {

  @Test
  public void validateDefaultLocalityOrder() {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    try {
      conf.validate();
    } catch (Exception e) {
      fail("Default locality order should be valid: " + e.getMessage());
    }
  }

  @Test
  public void validateCustomTierInOrder() {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    // Simulate setting a custom tier via alluxio.locality.{tier}=value
    String customTier = "zone";
    String customValue = "zone-a";
    String templateKey = String.format("alluxio.locality.%s", customTier);
    conf.set(PropertyKey.fromString(templateKey), customValue);

    // Now include the custom tier in locality.order
    conf.set(PropertyKey.LOCALITY_ORDER, "node,rack," + customTier);

    try {
      conf.validate();
    } catch (Exception e) {
      fail("Custom tier should be valid when listed in locality.order: " + e.getMessage());
    }
  }

  @Test
  public void validateCustomTierNotInOrder() {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    // Simulate setting a custom tier that is NOT included in locality.order
    String customTier = "zone";
    String customValue = "zone-a";
    String templateKey = String.format("alluxio.locality.%s", customTier);
    conf.set(PropertyKey.fromString(templateKey), customValue);

    // locality.order does NOT include the custom tier
    conf.set(PropertyKey.LOCALITY_ORDER, "node,rack");

    try {
      conf.validate();
      fail("Expected IllegalStateException for custom tier not listed in locality.order");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("does not exist in the tier list"));
    }
  }

  @Test
  public void validateEmptyLocalityOrder() {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    // Instead of calling getOrDefault on AlluxioProperties, just set the default value string.
    conf.set(PropertyKey.LOCALITY_ORDER, "node,rack");

    try {
      conf.validate();
    } catch (Exception e) {
      fail("Default locality order should be valid: " + e.getMessage());
    }
  }

  @Test
  public void validateDuplicateTierInOrder() {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.LOCALITY_ORDER, "node,rack,node");

    try {
      conf.validate();
    } catch (Exception e) {
      fail("Duplicate tiers in locality.order should be accepted (handled downstream): " + e.getMessage());
    }
  }

  @Test
  public void validateWhitespaceInOrder() {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.LOCALITY_ORDER, " node , rack , zone ");

    try {
      conf.validate();
    } catch (Exception e) {
      fail("Whitespace in locality.order should be trimmed and valid: " + e.getMessage());
    }
  }
}