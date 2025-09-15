package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the configuration `alluxio.jvm.monitor.info.threshold`.
 */
public class JvmMonitorInfoThresholdTest {

  /**
   * Tests whether the configuration `alluxio.jvm.monitor.info.threshold` satisfies its value constraints and dependencies.
   */
  @Test
  public void testValidJvmMonitorInfoThresholdConfiguration() {
    // Step 1: Retrieve the configuration value from an instance of ServerConfiguration.
    InstancedConfiguration conf = InstancedConfiguration.defaults();
    long infoThresholdMs = conf.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);

    // Step 2: Based on the understood constraints, verify if the value satisfies the requirements.
    // Constraint: The value must be non-negative since the configuration represents an interval in milliseconds.
    Assert.assertTrue(
        "Configuration 'alluxio.jvm.monitor.info.threshold' must be non-negative.",
        infoThresholdMs >= 0);

    // Step 3: Check additional dependencies. If the value of infoThresholdMs is greater than warnThresholdMs (assuming this is constrained in the source), that would be invalid.
    long warnThresholdMs = conf.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
    Assert.assertTrue(
        "Configuration 'alluxio.jvm.monitor.info.threshold' must be less than or equal to 'alluxio.jvm.monitor.warn.threshold'.",
        infoThresholdMs <= warnThresholdMs);

    // Step 4: Ensure that valid configuration does not cause misbehavior.
    // No additional checks are required for this configuration since its constraints are straightforward.
  }
}