package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for validating the configuration `hbase.server.compactchecker.interval.multiplier`.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestCompactionCheckMultiplierConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionCheckMultiplierConfig.class);

  private Configuration conf;

  /**
   * Initializes the test configuration.
   */
  @Before
  public void setUp() {
    // Correctly setup Configuration without relying on HBaseTestingUtility (since it is unavailable).
    conf = new Configuration();
    conf.setInt(HStore.COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY, HStore.DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);
  }

  /**
   * Test to validate the constraints of the `hbase.server.compactchecker.interval.multiplier` configuration.
   */
  @Test
  public void testCompactionCheckerIntervalMultiplier() {
    try {
      // Retrieve the configuration value using the correct API.
      int multiplier = conf.getInt(
          HStore.COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY, HStore.DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);

      // Constraint 1: The value must be positive.
      assertTrue("Configuration value for hbase.server.compactchecker.interval.multiplier must be greater than 0.",
          multiplier > 0);

      // Additional constraints could be added if discovered in source or documentation.
    } catch (Exception e) {
      fail("Exception encountered while testing hbase.server.compactchecker.interval.multiplier: " + e.getMessage());
    }
  }

  // Additional test cases can be added here if more constraints or dependencies arise.
}