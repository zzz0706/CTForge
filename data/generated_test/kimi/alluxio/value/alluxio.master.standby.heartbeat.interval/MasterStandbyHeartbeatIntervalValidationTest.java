package alluxio.conf;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class MasterStandbyHeartbeatIntervalValidationTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    // Load configuration from site properties and JVM properties without injecting values in code
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @Test
  public void validateIntervalShorterThanTimeout() {
    // 1. Obtain configuration values via the Alluxio 2.1.0 API
    long interval = mConf.getMs(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL);
    long timeout  = mConf.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

    // 2. Prepare test condition: the interval must be strictly less than the timeout
    // 3. Test code: validate the constraint
    try {
      mConf.validate(); // triggers InstancedConfiguration.checkTimeouts()
    } catch (IllegalStateException e) {
      fail(String.format(
          "heartbeat interval (%s=%d ms) must be less than heartbeat timeout (%s=%d ms)",
          PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL.getName(), interval,
          PropertyKey.MASTER_HEARTBEAT_TIMEOUT.getName(), timeout));
    }

    // 4. Post-test: ensure constraint holds
    assertTrue("heartbeat interval must be < heartbeat timeout", interval < timeout);
  }

  @After
  public void after() {
    mConf = null;
  }
}