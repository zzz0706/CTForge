package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;
import static org.junit.Assert.*;

public class MasterWorkerConnectWaitTimeValidationTest {

  @Test
  public void testWorkerConnectWaitTimeValid() {
    // 1. Obtain configuration values via Alluxio 2.1.0 API
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare test conditions – ensure validation is enabled
    conf.set(PropertyKey.CONF_VALIDATION_ENABLED, true);

    // 3. Test code – validate the configuration
    try {
      conf.validate();               // will throw if any check fails
      long waitTime = conf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
      long retryInterval = conf.getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

      // Explicitly enforce documented constraint
      assertTrue(
          "alluxio.master.worker.connect.wait.time must be >= alluxio.user.rpc.retry.max.sleep.ms",
          waitTime >= retryInterval);
    } catch (IllegalStateException e) {
      fail("Invalid configuration detected: " + e.getMessage());
    }
  }
}