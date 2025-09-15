package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestCompactionConfigurationValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = 
      HBaseClassTestRule.forClass(TestCompactionConfigurationValidation.class);

  /**
   * Unit test to validate the configuration value of "hbase.hstore.compaction.max.size".
   * Ensure it adheres to expected constraints.
   */
  @Test
  public void testHStoreCompactionMaxSizeConfiguration() {
    Configuration conf = new Configuration();

    // 1. Retrieve the configuration value using HBase 2.2.2 APIs.
    // Default value is Long.MAX_VALUE (9223372036854775807).
    long maxCompactSize = conf.getLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY, Long.MAX_VALUE);

    // 2. Verify that the configuration value is valid.
    // Constraints:
    // - Must be a positive long value (greater than zero).
    // - Must not exceed Long.MAX_VALUE.

    assertTrue("Configuration 'hbase.hstore.compaction.max.size' should be positive.",
        maxCompactSize > 0);
    assertTrue("Configuration 'hbase.hstore.compaction.max.size' should not exceed Long.MAX_VALUE.",
        maxCompactSize <= Long.MAX_VALUE);
  }
}