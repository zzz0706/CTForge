package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.FlushLargeStoresPolicy;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({org.apache.hadoop.hbase.testclassification.RegionServerTests.class,
           org.apache.hadoop.hbase.testclassification.SmallTests.class})
public class TestRegionServerConfigurationValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerConfigurationValidation.class);

  /**
   * Test case to validate the configuration value for 
   * `hbase.hregion.percolumnfamilyflush.size.lower.bound.min`.
   */
  @Test
  public void testColumnFamilyFlushSizeLowerBoundMinValidation() {
    // 1. Initialize HBase configuration
    Configuration conf = HBaseConfiguration.create();

    // Fetch configuration values using the correct HBase 2.2.2 API
    long lowerBoundMin = conf.getLong(
        FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
        FlushLargeStoresPolicy.DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN);

    long memStoreFlushSize = conf.getLong(
        "hbase.regionserver.global.memstore.size.lower.limit", 128L * 1024 * 1024); // updated to correct key and assumed default value

    // 2. Prepare the test conditions
    int sampleFamilyCount = 4; // Example number of column families
    long computedLowerBound = memStoreFlushSize / sampleFamilyCount;

    // Ensure computedLowerBound is non-negative before applying assertions
    computedLowerBound = Math.max(computedLowerBound, 0);

    // 3. Test code: Apply assertions based on computed values and actual results
    assertTrue(
        "The lower bound minimum (`hbase.hregion.percolumnfamilyflush.size.lower.bound.min`) should be non-negative.",
        lowerBoundMin >= 0);

    assertTrue(
        "The lower bound minimum (`hbase.hregion.percolumnfamilyflush.size.lower.bound.min`) should not exceed the computed flush size lower bound.",
        lowerBoundMin <= computedLowerBound);

    // 4. Code after testing: Log for debugging purposes
    System.out.println("Configuration validation for `hbase.hregion.percolumnfamilyflush.size.lower.bound.min` passed: " +
        lowerBoundMin + " is within acceptable range compared to computed value: " + computedLowerBound);
  }
}