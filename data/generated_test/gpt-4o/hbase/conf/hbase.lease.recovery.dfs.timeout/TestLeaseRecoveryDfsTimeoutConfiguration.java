package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for validating the correctness of the configuration hbase.lease.recovery.dfs.timeout
 * and its constraints in HBase 2.2.2.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestLeaseRecoveryDfsTimeoutConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLeaseRecoveryDfsTimeoutConfiguration.class);

  private static Configuration conf;

  @BeforeClass
  public static void setup() {
    // Initialize the Configuration object using HBaseConfiguration
    // This prepares the necessary configuration for testing.
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testValidHbaseLeaseRecoveryDfsTimeoutConfiguration() {
    try {
      // 1. Retrieve the value of the configuration directly from the Configuration object
      long dfsTimeout = conf.getLong("hbase.lease.recovery.dfs.timeout", 64000L);

      // 2. Validate that the value is greater than zero. Timeout values should be positive.
      assertTrue("hbase.lease.recovery.dfs.timeout must be positive", dfsTimeout > 0);

      // 3. Validate that the value aligns with the expected range and constraints
      // Constraint: dfsTimeout should generally be significantly larger than dfs.heartbeat.interval
      long dfsHeartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3000L); // Example default value
      assertTrue("hbase.lease.recovery.dfs.timeout should be larger than dfs.heartbeat.interval",
          dfsTimeout > dfsHeartbeatInterval);

      // Constraint: dfsTimeout should be greater than dfs.client.socket-timeout
      long clientSocketTimeout = conf.getLong("dfs.client.socket-timeout", 60000L); // Example default value
      assertTrue("hbase.lease.recovery.dfs.timeout should be larger than dfs.client.socket-timeout",
          dfsTimeout > clientSocketTimeout);

    } catch (Exception e) {
      // 4. Handle any exceptions properly by failing the test with the error message
      fail("Exception occurred during validation of hbase.lease.recovery.dfs.timeout: " + e.getMessage());
    }
  }
}