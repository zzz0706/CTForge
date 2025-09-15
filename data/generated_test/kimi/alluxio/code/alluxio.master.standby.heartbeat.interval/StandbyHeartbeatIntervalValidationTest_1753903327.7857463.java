package alluxio.conf;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Test;

public class StandbyHeartbeatIntervalValidationTest {

  @After
  public void tearDown() {
    // Clear any system properties that may have been set during the test
    System.clearProperty(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL.toString());
    System.clearProperty(PropertyKey.MASTER_HEARTBEAT_TIMEOUT.toString());
  }

  @Test
  public void validate_intervalEqualsTimeout_shouldFail() {
    // 1. Create a fresh InstancedConfiguration instance
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Compute the timeout value from the default configuration
    long timeoutMs = conf.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

    // 3. Set the interval to equal the timeout
    conf.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, timeoutMs + "ms");

    // 4. Invoke validation and expect failure
    try {
      conf.validate();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      // 5. Verify the message
      assertTrue(e.getMessage().contains(
          "heartbeat interval (" + PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL + "=" + timeoutMs
          + ") must be less than heartbeat timeout (" + PropertyKey.MASTER_HEARTBEAT_TIMEOUT + "="
          + timeoutMs + ")"));
    }
  }

  @Test
  public void validate_intervalLessThanTimeout_shouldPass() {
    // 1. Create a fresh InstancedConfiguration instance
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Compute the timeout value from the default configuration
    long timeoutMs = conf.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

    // 3. Set the interval to be less than the timeout
    conf.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, (timeoutMs - 1) + "ms");

    // 4. Invoke validation and expect success
    conf.validate(); // Should not throw
  }

  @Test
  public void validate_intervalGreaterThanTimeout_shouldFail() {
    // 1. Create a fresh InstancedConfiguration instance
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Compute the timeout value from the default configuration
    long timeoutMs = conf.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

    // 3. Set the interval to be greater than the timeout
    conf.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, (timeoutMs + 1) + "ms");

    // 4. Invoke validation and expect failure
    try {
      conf.validate();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      // 5. Verify the message
      assertTrue(e.getMessage().contains(
          "heartbeat interval (" + PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL + "="
          + (timeoutMs + 1) + ") must be less than heartbeat timeout ("
          + PropertyKey.MASTER_HEARTBEAT_TIMEOUT + "=" + timeoutMs + ")"));
    }
  }
}