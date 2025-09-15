package org.apache.hadoop.hbase.mob; 

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for verifying the configuration values related to the Expired Mob File Cleaner.
 */
@Category(SmallTests.class)
public class TestExpiredMobFileCleanerConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExpiredMobFileCleanerConfig.class);

  private static Configuration configuration;

  /**
   * Set up the test configuration using HBaseConfiguration.
   */
  @BeforeClass
  public static void setup() throws Exception {
    // Create configuration instance using the HBaseConfiguration utility
    configuration = HBaseConfiguration.create();
    
    // Set a relevant test configuration value for mob.ttl.cleaner.period
    configuration.setInt("hbase.master.mob.ttl.cleaner.period", 86400);  // Example configuration: 1 day in seconds
  }

  /**
   * Test to validate the mob cleaner period configuration value. 
   * Ensures that the retrieved configuration value satisfies the expected conditions.
   */
  @Test
  public void testMobCleanerPeriodConfigValid() {
    // Retrieving the mob cleaner period value using the test configuration
    int mobCleanerPeriod = configuration.getInt(
        "hbase.master.mob.ttl.cleaner.period", 0); // Default value fallback: 0 seconds

    try {
      // Validate that the retrieved configuration value is positive and within the permissible range
      assertTrue("ExpiredMobFileCleaner period must be positive and within the valid range", 
          mobCleanerPeriod > 0 && mobCleanerPeriod <= 86400);  // Range example: Up to 1 day
    } catch (Exception e) {
      fail("Test failed unexpectedly due to exception: " + e.getMessage());
    }
  }
}