package org.apache.hadoop.hbase.conf;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestBulkloadRetriesNumberConfig {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestBulkloadRetriesNumberConfig.class);

  /**
   * Validate that hbase.bulkload.retries.number is a non-negative integer.
   * 0 means "never give up" (infinite retries), any positive value is the
   * maximum number of retries, negative values are invalid.
   */
  @Test
  public void testBulkloadRetriesNumberIsValid() {
    Configuration conf = HBaseConfiguration.create();
    int retries = conf.getInt(HConstants.BULKLOAD_MAX_RETRIES_NUMBER, 10);
    assertTrue(
        "hbase.bulkload.retries.number must be a non-negative integer",
        retries >= 0);
  }
}