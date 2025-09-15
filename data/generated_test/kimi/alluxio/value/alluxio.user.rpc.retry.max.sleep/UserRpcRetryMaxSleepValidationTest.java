package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.Constants;

import org.junit.Assert;
import org.junit.Test;

public class UserRpcRetryMaxSleepValidationTest {

  @Test
  public void validateUserRpcRetryMaxSleep() {
    // 1. Use Alluxio 2.1.0 API to load the configuration
    InstancedConfiguration conf = InstancedConfiguration.defaults();

    // 2. Retrieve the value of alluxio.user.rpc.retry.max.sleep
    long maxSleepMs = conf.getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

    // 3. Validate the configuration value
    // 3.1 Ensure it is a positive duration (>= 0)
    Assert.assertTrue(
        PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS + " must be non-negative",
        maxSleepMs >= 0);

    // 3.2 Ensure it does not exceed any reasonable upper bound (sanity check)
    //     The default is 3 seconds; we allow up to 1 hour (3,600,000 ms) as a loose upper limit.
    Assert.assertTrue(
        PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS + " must not exceed 1 hour",
        maxSleepMs <= 3600 * Constants.SECOND_MS);

    // 3.3 Validate the dependency with MASTER_WORKER_CONNECT_WAIT_TIME
    long masterWorkerConnectWaitMs = conf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
    if (masterWorkerConnectWaitMs < maxSleepMs) {
      // Although the code only logs a warning, we still flag it as an invalid configuration
      Assert.fail(String.format(
          "%s (%d ms) is smaller than %s (%d ms). "
              + "Workers might not have enough time to register.",
          PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, masterWorkerConnectWaitMs,
          PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, maxSleepMs));
    }
  }
}