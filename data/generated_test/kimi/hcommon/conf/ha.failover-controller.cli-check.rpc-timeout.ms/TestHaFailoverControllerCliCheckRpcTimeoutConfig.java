package org.apache.hadoop.ha;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHaFailoverControllerCliCheckRpcTimeoutConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testCliCheckRpcTimeoutMsIsPositiveInt() {
    // 1. Obtain the configuration value using the hdfs 2.8.5 API
    int timeout = conf.getInt(
        CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);

    // 2. Test conditions: none; we only validate the loaded value
    // 3. Test code: ensure the value is a positive integer
    assertTrue("ha.failover-controller.cli-check.rpc-timeout.ms must be a positive integer",
               timeout > 0);
  }
}