package alluxio.conf;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {

  /**
   * Test to validate the constraints and dependencies of the configuration 
   * `alluxio.master.worker.connect.wait.time`.
   */
  @Test
  public void validateMasterWorkerConnectWaitTime() {
    // Obtain the Alluxio configuration instance
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();

    // Step 1: Retrieve the value of the configuration `alluxio.master.worker.connect.wait.time`
    long waitTime = conf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);

    // Step 2: Retrieve the value of dependent configuration `alluxio.user.rpc.retry.max.sleep.ms`
    long retryInterval = conf.getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

    // Step 3: Validate the range and constraints for `alluxio.master.worker.connect.wait.time`
    // Ensure waitTime is not less than retryInterval
    Assert.assertTrue(
        String.format(
            "%s=%dms is smaller than %s=%dms. Workers might not have enough time to register.",
            PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, waitTime,
            PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, retryInterval),
        waitTime >= retryInterval);

    // Additional validations could be added if needed to cover further dependencies.
  }
}