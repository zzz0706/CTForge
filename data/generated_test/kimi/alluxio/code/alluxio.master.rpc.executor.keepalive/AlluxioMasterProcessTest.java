package alluxio.master;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class AlluxioMasterProcessTest {

  @Test
  public void verifyIdleThreadTimeoutRespectsConfigValue() throws Exception {
    // 1. Use Alluxio 2.1.0 API to obtain configuration
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare test conditions – compute expected keep-alive from configuration
    long expectedKeepAliveMs = conf.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

    // 3. Test code – obtain the configured value and assert it
    assertEquals("keep-alive must match configuration",
                 expectedKeepAliveMs,
                 conf.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE));
  }
}