package alluxio.master.block;

import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;

public class MasterWorkerTimeoutValidationTest {

  @Test
  public void validateMasterWorkerTimeout() {
    // 1. Obtain configuration values via the Alluxio 2.1.0 API
    AlluxioConfiguration conf = new InstancedConfiguration(new alluxio.conf.AlluxioProperties());

    // 2. Prepare the test conditions: read the value set in the configuration file
    String timeoutStr = conf.get(PropertyKey.MASTER_WORKER_TIMEOUT_MS);

    // 3. Test code: verify the value satisfies constraints
    long timeoutMs;
    try {
      timeoutMs = conf.getMs(PropertyKey.MASTER_WORKER_TIMEOUT_MS);
    } catch (IllegalArgumentException e) {
      assertTrue("Invalid time-unit suffix in alluxio.master.worker.timeout", false);
      return;
    }

    // The value must be a positive duration (greater than 0)
    assertTrue("alluxio.master.worker.timeout must be positive", timeoutMs > 0);

    // 4. Code after testing: no teardown required
  }
}