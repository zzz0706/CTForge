package org.apache.hadoop.ha;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class TestHAAdminCliCheckRpcTimeoutConfig {

  @Test
  public void testCliCheckRpcTimeoutIsPositiveInt() {
    Configuration conf = new Configuration(false);
    conf.addResource("core-site.xml");   // loads from classpath

    int timeout = conf.getInt(
        CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);

    assertTrue("ha.failover-controller.cli-check.rpc-timeout.ms must be a positive integer",
               timeout > 0);
  }
}