package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestHaFailoverControllerGracefulFenceConnectionRetries {

  @Test
  public void testGracefulFenceConnectionRetriesValid() {
    // 1. Obtain configuration values via the HDFS 2.8.5 API
    Configuration conf = new Configuration(false);
    conf.addResource("hdfs-site.xml");
    conf.addResource("core-site.xml");

    // 2. Prepare the test conditions
    int retries = conf.getInt(
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES,
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT);

    // 3. Test code – validate the value
    //    - Must be a non-negative integer
    assertTrue("ha.failover-controller.graceful-fence.connection.retries must be >= 0",
               retries >= 0);

    // 4. No teardown necessary
  }
}