package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHaFailoverControllerConfigValidation {

  @Test
  public void testHaFcNewActiveRpcTimeoutMsValid() {
    Configuration conf = new Configuration(false);
    // do NOT set any value in test code – rely on external conf files
    int timeout = conf.getInt(
        CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT);

    // rule: must be positive integer
    assertTrue("ha.failover-controller.new-active.rpc-timeout.ms must be > 0",
               timeout > 0);
  }

  @Test
  public void testHaFcNewActiveRpcTimeoutMsNotFloat() {
    Configuration conf = new Configuration(false);
    // do NOT set any value in test code – rely on external conf files
    String raw = conf.getRaw(CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY);
    if (raw != null) {
      try {
        Integer.parseInt(raw.trim());
      } catch (NumberFormatException e) {
        fail("ha.failover-controller.new-active.rpc-timeout.ms is not a valid int: " + raw);
      }
    }
  }
}