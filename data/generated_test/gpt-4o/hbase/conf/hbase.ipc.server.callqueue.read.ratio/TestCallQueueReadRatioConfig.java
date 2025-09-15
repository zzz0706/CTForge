package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit test for validating HBase IPC server configuration: `hbase.ipc.server.callqueue.read.ratio`.
 */
@Category({RPCTests.class, SmallTests.class})
public class TestCallQueueReadRatioConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCallQueueReadRatioConfig.class);

  private static Configuration configuration;

  @BeforeClass
  public static void setUp() throws Exception {
    // Initialize the configuration using HBaseConfiguration
    configuration = HBaseConfiguration.create();
  }

  /**
   * Test to validate that `hbase.ipc.server.callqueue.read.ratio` configuration respects the 
   * allowed value range and constraints.
   */
  @Test
  public void testReadRatioConfigValidity() {
    // Prepare test conditions: Obtain the configuration value for CALL_QUEUE_READ_SHARE_CONF_KEY
    float readRatio = configuration.getFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0);

    // Test the validity of the configuration value.
    try {
      // The allowed range for the read ratio is [0.0, 1.0].
      assertTrue("hbase.ipc.server.callqueue.read.ratio must be in the range [0.0, 1.0]",
          readRatio >= 0.0f && readRatio <= 1.0f);

    } catch (AssertionError e) {
      // Fail the test if the value is invalid and append the error message.
      fail("Invalid configuration for hbase.ipc.server.callqueue.read.ratio: " + e.getMessage());
    }
  }
}