package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for validating the configuration {@code alluxio.jvm.monitor.warn.threshold}.
 */
public class JvmMonitorWarnThresholdConfigTest {

  @Test
  public void warnThresholdMustBePositiveDuration() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    long valueMs = conf.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);

    // 2. Prepare the test conditions.
    // (value already obtained above)

    // 3. Test code.
    Assert.assertTrue(
        "alluxio.jvm.monitor.warn.threshold must be a positive duration (ms > 0)",
        valueMs > 0);

    // 4. Code after testing.
  }
}