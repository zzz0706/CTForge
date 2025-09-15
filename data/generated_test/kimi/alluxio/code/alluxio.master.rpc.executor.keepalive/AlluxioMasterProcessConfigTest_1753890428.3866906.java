package alluxio.master;

import static org.junit.Assert.assertEquals;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AlluxioMasterProcessConfigTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    // 1. Use the alluxio2.1.0 API correctly to obtain configuration values.
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
    // Ensure the property is NOT overridden in any resource (clean configuration).
    mConf.unset(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
    ServerConfiguration.reset();
    ServerConfiguration.global().merge(mConf.toMap(), alluxio.conf.Source.RUNTIME);
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void verifyDefaultValueLoadedWhenNoOverride() {
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    // Verify that the default value is correctly loaded when no override is provided.
    long expectedKeepAliveMs = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
    assertEquals(60000L, expectedKeepAliveMs);
  }
}