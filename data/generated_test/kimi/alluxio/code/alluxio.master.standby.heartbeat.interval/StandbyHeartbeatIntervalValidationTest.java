package alluxio.conf;

import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

public class StandbyHeartbeatIntervalValidationTest {

  @Test
  public void validate_intervalEqualsTimeout_shouldFail() {
    // 1. Create a fresh InstancedConfiguration instance
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Compute the timeout value from the default configuration
    long timeoutMs = conf.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

    // 3. Set the interval to equal the timeout
    conf.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, timeoutMs + "ms");

    // 4. Invoke validation and expect failure
    IllegalStateException thrown = null;
    try {
      conf.validate();
    } catch (IllegalStateException e) {
      thrown = e;
    }

    // 5. Verify the message
    assertTrue(thrown.getMessage().contains(
        "heartbeat interval (" + PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL + "=" + timeoutMs
        + ") must be less than heartbeat timeout (" + PropertyKey.MASTER_HEARTBEAT_TIMEOUT + "="
        + timeoutMs + ")"));
  }
}