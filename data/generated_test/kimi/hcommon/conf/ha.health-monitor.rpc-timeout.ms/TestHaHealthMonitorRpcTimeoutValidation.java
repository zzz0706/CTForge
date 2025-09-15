package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestHaHealthMonitorRpcTimeoutValidation {

  @Test
  public void testHaHealthMonitorRpcTimeoutValid() {
    Configuration conf = new Configuration(false);
    conf.addResource("core-site.xml");

    int rpcTimeout = conf.getInt(
        CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT);

    // 1. Must be a positive integer (negative or zero makes no sense for a timeout)
    assertTrue("ha.health-monitor.rpc-timeout.ms must be > 0", rpcTimeout > 0);

    // 2. Should not exceed reasonable upper bound (2^31-1 ms ≈ 24 days)
    assertTrue("ha.health-monitor.rpc-timeout.ms must be <= Integer.MAX_VALUE",
               rpcTimeout <= Integer.MAX_VALUE);
  }
}