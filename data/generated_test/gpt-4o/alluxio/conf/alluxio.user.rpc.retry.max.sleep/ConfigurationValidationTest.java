package alluxio.test;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {
  
  @Test
  public void testUserRpcRetryMaxSleepConstraint() {
    /*
    * Step 1: Based on the understood constraints and dependencies, determine whether the read configuration value satisfies the constraints and dependencies.
    * Step 2: Verify whether the value of this configuration item satisfies the constraints and dependencies.
    *         1. Ensure the configuration value is a valid duration format (e.g., numeric followed by a time unit like "sec", "ms").
    *         2. Ensure the value is a non-negative duration.
    *         3. Check dependencies on other configurations, such as MASTER_WORKER_CONNECT_WAIT_TIME.
    */
    
    // Obtain the configuration instance
    InstancedConfiguration conf = InstancedConfiguration.defaults();

    // Test the specific configuration value
    long maxSleepDurationMs = conf.getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

    // Step 2.1 Validate that the value is non-negative
    Assert.assertTrue(
        "alluxio.user.rpc.retry.max.sleep must be non-negative.",
        maxSleepDurationMs >= 0);

    // Step 2.2 Validate dependency with MASTER_WORKER_CONNECT_WAIT_TIME
    long connectWaitTimeMs = conf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
    Assert.assertTrue(
        String.format(
          "%s (%d ms) must not be smaller than %s (%d ms).",
          PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME,
          connectWaitTimeMs,
          PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS,
          maxSleepDurationMs),
        connectWaitTimeMs >= maxSleepDurationMs);
  }
}