package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MasterRpcExecutorKeepaliveValidationTest {

  @Test
  public void testKeepalivePositive() {
    // 1. Load configuration from default properties
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Read the configuration value
    long keepaliveMs = conf.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

    // 3. Validate that the keepalive time is strictly positive
    assertTrue("alluxio.master.rpc.executor.keepalive must be > 0",
               keepaliveMs > 0L);
  }
}