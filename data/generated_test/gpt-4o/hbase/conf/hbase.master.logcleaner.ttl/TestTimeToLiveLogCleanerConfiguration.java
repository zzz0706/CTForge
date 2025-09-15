package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@Category(SmallTests.class)
public class TestTimeToLiveLogCleanerConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimeToLiveLogCleanerConfiguration.class);

  private Configuration configuration;

  @Before
  public void setUp() {
    configuration = HBaseConfiguration.create();
  }

  /**
   * Test to validate the configuration value for hbase.master.logcleaner.ttl
   */
  @Test
  public void testTTLConfiguration() {
    // Obtain configuration value using HBase's API
    long ttl = configuration.getLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 600000);

    // Prepare test conditions
    // Ensure the TTL is a positive value; a negative or zero value is invalid.
    assertTrue("TTL configuration value must be greater than zero", ttl > 0);

    // Additional validation
    // Example: If TTL is too small (e.g., less than 5 seconds), which could be impractical
    long minTTL = 5000; // Minimum allowed TTL in milliseconds
    assertTrue("TTL configuration value must be meaningful (greater than " + minTTL + " ms)", ttl >= minTTL);

    // Example of an upper bound
    long maxTTL = 86400000; // Maximum allowed TTL in milliseconds (1 day)
    assertTrue("TTL configuration value must be within a reasonable range (less than " + maxTTL + " ms)", ttl <= maxTTL);

    // Edge case: If HBase detects the value cannot handle future clock skew, ensure valid handling
    long currentTime = System.currentTimeMillis();
    long skewedTime = currentTime - ttl; // Simulate behavior with the current configuration
    if (currentTime < skewedTime) {
      assertFalse("TTL cannot result in negative lifecycle management due to clock skew", currentTime < skewedTime);
    }
  }
}