package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for validating the configuration {@code alluxio.worker.block.heartbeat.interval}.
 */
public class WorkerBlockHeartbeatIntervalConfigValidationTest {

  @Test
  public void validateWorkerBlockHeartbeatInterval() {
    // 1. Obtain the configuration from the default file without setting any value in code.
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Retrieve the configured value.
    long intervalMs = conf.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS);

    // 3. Validate the value:
    //    - Must be a positive duration (greater than 0 ms).
    //    - Must be convertible to a non-negative integer when cast to int
    //      (used in SessionCleaner and HeartbeatThread).
    assertTrue("alluxio.worker.block.heartbeat.interval must be > 0 ms",
        intervalMs > 0);
    assertTrue("alluxio.worker.block.heartbeat.interval must fit in a 32-bit signed int",
        intervalMs <= Integer.MAX_VALUE);
  }
}