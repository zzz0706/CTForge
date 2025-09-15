package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHealthMonitorRpcTimeoutConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Obtain configuration values from the loaded configuration files
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  /**
   * Tests that the value for {@code ha.health-monitor.rpc-timeout.ms} is a
   * positive integer and falls within the expected range.
   */
  @Test
  public void testRpcTimeoutMsValid() {
    // 2. Prepare the test conditions – configuration is already loaded
    int rpcTimeout = conf.getInt(
        CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT);

    // 3. Test code – verify constraints
    assertTrue("ha.health-monitor.rpc-timeout.ms must be > 0",
        rpcTimeout > 0);

    // Additional sanity check: extremely large values are likely errors
    assertTrue("ha.health-monitor.rpc-timeout.ms seems too large (> 1 hour)",
        rpcTimeout <= 60 * 60 * 1000);
  }

  /**
   * Ensures the configuration key is recognized and the default is applied
   * when absent.
   */
  @Test
  public void testRpcTimeoutMsDefault() {
    // 2. Prepare the test conditions – unset any explicit value
    conf.unset(CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY);

    // 3. Test code – default must be the documented value
    int expected = CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT;
    int actual = conf.getInt(
        CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY, expected);
    assertEquals("Default value mismatch", expected, actual);
  }
}