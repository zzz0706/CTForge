package alluxio.conf;

import alluxio.Constants;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for validating {@code alluxio.jvm.monitor.info.threshold}.
 */
public class JvmMonitorInfoThresholdValidationTest {

  @Test
  public void validateJvmMonitorInfoThreshold() {
    // 1. Load configuration from the file without hard-coding any value
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Retrieve the configuration value
    String key = "alluxio.jvm.monitor.info.threshold";
    String rawValue = conf.get(PropertyKey.fromString(key));
    long infoThresholdMs = conf.getMs(PropertyKey.fromString(key));

    // 3. Validate constraints
    //    a) Must be a valid time duration (parseable by getMs)
    //    b) Must be >= 0 (negative durations are invalid)
    //    c) Must be < warn threshold (warn threshold must be strictly larger)
    long warnThresholdMs = conf.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);

    Assert.assertNotNull(rawValue);
    Assert.assertTrue("alluxio.jvm.monitor.info.threshold must be non-negative", infoThresholdMs >= 0);
    Assert.assertTrue(
        "alluxio.jvm.monitor.info.threshold must be less than warn threshold",
        infoThresholdMs < warnThresholdMs);
  }
}