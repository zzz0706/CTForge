package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FailoverControllerConfigTest {

  @Test
  public void testZeroRetriesPropagatedToIPCConfig() throws Exception {
    // 1. Create Configuration and set the property to 0
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES, 0);

    // 2. Instantiate FailoverController with a dummy RequestSource
    //    In 2.8.5 the FailoverController constructor needs a RequestSource
    FailoverController fc = new FailoverController(conf, RequestSource.REQUEST_BY_USER);

    // 3. Read the values propagated into gracefulFenceConf via reflection
    java.lang.reflect.Field gracefulFenceConfField =
            FailoverController.class.getDeclaredField("gracefulFenceConf");
    gracefulFenceConfField.setAccessible(true);
    Configuration gracefulFenceConf = (Configuration) gracefulFenceConfField.get(fc);

    int actualRetries = gracefulFenceConf.getInt(
            CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, -1);
    int actualRetriesOnTimeouts = gracefulFenceConf.getInt(
            CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, -1);

    // 4. Assert both IPC keys are set to 0
    assertEquals("IPC_CLIENT_CONNECT_MAX_RETRIES should be 0", 0, actualRetries);
    assertEquals("IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY should be 0",
            0, actualRetriesOnTimeouts);
  }
}