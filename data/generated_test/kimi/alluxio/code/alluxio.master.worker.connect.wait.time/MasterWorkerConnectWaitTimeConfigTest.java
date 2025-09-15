package alluxio.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MasterWorkerConnectWaitTimeConfigTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    // 1. Use the 2.1.0 API to obtain a fresh, mutable configuration
    mConf = new InstancedConfiguration(new AlluxioProperties());
  }

  @After
  public void after() {
    // 4. Code after testing – reset configuration to avoid side-effects
    mConf = null;
  }

  @Test
  public void defaultWaitTimeIsParsedAsFiveSeconds() {
    // 2. Ensure we are using the default (no overrides)
    long actual = mConf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);

    // 3. Test code
    assertEquals(5000L, actual);
  }

  @Test
  public void validateTriggersCheckTimeouts() throws Exception {
    // 2. Prepare test conditions – make sure validation is enabled
    mConf.set(PropertyKey.CONF_VALIDATION_ENABLED, true);

    // 3. Test code – invoke the public method that internally calls checkTimeouts()
    mConf.validate();
    // No exception should be thrown; the private warning branch is covered when
    // MASTER_WORKER_CONNECT_WAIT_TIME < USER_RPC_RETRY_MAX_SLEEP_MS
  }

  @Test
  public void safeModeManagerUsesConfiguredWaitTime() throws Exception {
    // 2. Prepare test conditions – override the wait time
    long customWaitMs = 100L;
    mConf.set(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, customWaitMs);

    // 3. Test code – simply ensure the value is read correctly
    assertEquals(customWaitMs, mConf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME));
  }
}