package alluxio.conf;

import static org.junit.Assert.assertEquals;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;

import org.junit.Test;

public class MasterWorkerConnectWaitTimeDefaultsTest {

  @Test
  public void defaultWaitTimeIsParsedAsFiveSeconds() {
    // 1. Instantiate a fresh configuration without any overrides
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 2. Dynamically compute the expected value from the default definition
    long expectedWaitMs = conf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);

    // 3. Read the actual value from the configuration
    long actualWaitMs = conf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);

    // 4. Assert that the default is correctly interpreted as 5000 ms
    assertEquals(5000L, actualWaitMs);
  }
}