package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.heartbeat.HeartbeatThread;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MasterHeartbeatIntervalTest {

  @Before
  public void setUp() {
    ServerConfiguration.reset();
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_HEARTBEAT_INTERVAL, "5000ms");
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  @Test
  public void verifyCustomIntervalInMsIsRespected() {
    // 1. Use Alluxio 2.1.0 API to obtain the configuration value
    long expectedIntervalMs = ServerConfiguration.getMs(PropertyKey.MASTER_WORKER_HEARTBEAT_INTERVAL);

    // 2. Prepare the test conditions (configuration already set above)

    // 3. Test code – simply assert that the configuration is read correctly
    assertEquals(5000L, expectedIntervalMs);

    // 4. Code after testing (tearDown will reset configuration)
  }
}