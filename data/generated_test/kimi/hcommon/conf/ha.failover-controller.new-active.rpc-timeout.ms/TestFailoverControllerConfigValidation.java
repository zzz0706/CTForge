package org.apache.hadoop.ha;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class TestFailoverControllerConfigValidation {

  @Test
  public void testRpcTimeoutToNewActiveConfig() {
    Configuration conf = new Configuration(false);

    // 1. Read the configuration value from the test resource file
    //    (the file must be present in the test classpath).
    conf.addResource("core-site.xml");

    // 2. Obtain the value through the same API used by production code
    int timeout = FailoverController.getRpcTimeoutToNewActive(conf);

    // 3. Validate constraints:
    //    - Must be a positive integer
    //    - Must not exceed reasonable upper bound (30 min = 1,800,000 ms)
    assertTrue("ha.failover-controller.new-active.rpc-timeout.ms must be > 0",
               timeout > 0);
    assertTrue("ha.failover-controller.new-active.rpc-timeout.ms must be <= 1,800,000",
               timeout <= 1_800_000);
  }
}