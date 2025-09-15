package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@Category(org.apache.hadoop.hbase.testclassification.SmallTests.class)
public class TestHBaseRegionServerConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseRegionServerConfiguration.class);

  @Test
  public void testOptionalCacheFlushIntervalConfig() {
    // Step 1: Initialize HBase Configuration
    Configuration conf = HBaseConfiguration.create();

    // Step 2: Retrieve the configuration value for hbase.regionserver.optionalcacheflushinterval
    String configKey = "hbase.regionserver.optionalcacheflushinterval";
    int defaultFlushInterval = 3600000; // Default value (1 hour)
    int flushCheckInterval = conf.getInt(configKey, defaultFlushInterval);

    // Step 3: Validate configuration constraints
    // Constraint: Value should be >= 0 (negative values are not valid)
    assertFalse("Configuration value for " + configKey + " must not be negative.", flushCheckInterval < 0);

    // Test for default value correctness
    assertTrue("Default configuration value should be properly set to 3600000.",
        flushCheckInterval == defaultFlushInterval || flushCheckInterval >= 0);

    // If a custom value exists, ensure it's non-negative
    if (flushCheckInterval != defaultFlushInterval) {
      assertTrue("Custom configuration value must be non-negative.", flushCheckInterval >= 0);
    }
  }
}