package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import com.google.common.base.Preconditions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationValidationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationValidationTest.class);

  @Test
  public void testMasterStandbyHeartbeatIntervalValidation() {
    // Step 1: Obtain configuration values using InstancedConfiguration API.
    AlluxioConfiguration configuration = InstancedConfiguration.defaults();
    long heartbeatIntervalMs = configuration.getMs(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL);
    long heartbeatTimeoutMs = configuration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

    // Step 2: Verify whether the values satisfy the constraints and dependencies.
    // Check that heartbeat interval is less than the heartbeat timeout.
    try {
      Preconditions.checkState(
          heartbeatIntervalMs < heartbeatTimeoutMs,
          String.format(
              "Heartbeat interval (%s=%d ms) must be less than heartbeat timeout (%s=%d ms)",
              PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL,
              heartbeatIntervalMs,
              PropertyKey.MASTER_HEARTBEAT_TIMEOUT,
              heartbeatTimeoutMs));
    } catch (IllegalStateException e) {
      LOG.error("Configuration validation failed: {}", e.getMessage());
      throw e; // Rethrow exception to indicate test failure.
    }

    LOG.info("Configuration validation passed: Heartbeat interval < Heartbeat timeout");
  }
}