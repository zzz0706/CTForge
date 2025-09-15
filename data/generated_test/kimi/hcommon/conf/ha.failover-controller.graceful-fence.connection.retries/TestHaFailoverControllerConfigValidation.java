package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHaFailoverControllerConfigValidation {

  @Test
  public void testGracefulFenceConnectionRetriesValid() {
    Configuration conf = new Configuration(false);
    // 1. Load from the default resource file (core-site.xml, etc.) instead of setting values
    conf.addResource("core-site.xml");

    // 2. Retrieve the value
    int retries = conf.getInt(
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES,
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT);

    // 3. Validate constraints: must be a non-negative integer
    assertTrue("ha.failover-controller.graceful-fence.connection.retries must be >= 0",
               retries >= 0);
  }
}