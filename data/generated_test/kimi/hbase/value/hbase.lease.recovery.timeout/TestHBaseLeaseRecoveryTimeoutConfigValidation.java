package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestHBaseLeaseRecoveryTimeoutConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseLeaseRecoveryTimeoutConfigValidation.class);

  private static final String CONFIG_KEY = "hbase.lease.recovery.timeout";

  /**
   * Tests that hbase.lease.recovery.timeout is a valid integer and positive.
   * The value represents a timeout in milliseconds; zero or negative values
   * would cause immediate timeout or undefined behaviour.
   */
  @Test
  public void testLeaseRecoveryTimeoutIsPositiveInt() throws IOException {
    Configuration conf = new Configuration(false);
    // Load the configuration as it would be read from hbase-site.xml
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");

    String valueStr = conf.get(CONFIG_KEY);
    if (valueStr != null) {
      try {
        int timeoutMs = Integer.parseInt(valueStr.trim());
        assertTrue("hbase.lease.recovery.timeout must be > 0, found: " + timeoutMs,
                   timeoutMs > 0);
      } catch (NumberFormatException nfe) {
        throw new IOException("hbase.lease.recovery.timeout must be a valid integer", nfe);
      }
    }
  }
}