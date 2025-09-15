package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit test for validating the configuration constraint and usage for
 * "ha.health-monitor.rpc-timeout.ms" in the Hadoop Common 2.8.5.
 */
public class TestHAHealthMonitorRPCTimeout {

  @Test
  public void testHAHealthMonitorRPCTimeoutConfiguration() {
    // Step 1: Load configuration using Hadoop's Configuration class.
    Configuration conf = new Configuration();

    // Step 2: Retrieve the value of the ha.health-monitor.rpc-timeout.ms configuration.
    int rpcTimeout = conf.getInt(
        CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT);

    // Constraints:
    // - The configuration value should be a positive integer because it represents a timeout in milliseconds.
    
    // Validate that the value is greater than 0 (timeout in milliseconds must be positive)
    assertTrue("Invalid value for ha.health-monitor.rpc-timeout.ms. It must be a positive integer.", 
        rpcTimeout > 0);
    
    // Additional: Validate default behavior if not explicitly set
    int defaultTimeout = 45000; // Default value based on the information provided
    if (conf.get(CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY) == null) {
      assertEquals("Default value for ha.health-monitor.rpc-timeout.ms is incorrect.", 
          defaultTimeout, rpcTimeout);
    }
  }
}