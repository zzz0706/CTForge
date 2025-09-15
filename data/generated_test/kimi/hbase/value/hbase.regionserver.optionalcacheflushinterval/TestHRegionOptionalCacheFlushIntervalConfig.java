package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionOptionalCacheFlushIntervalConfig {

  private Configuration conf;

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHRegionOptionalCacheFlushIntervalConfig.class);

  @Before
  public void setUp() {
    // 1. Use the hbase 2.2.2 API to load configuration from hbase-site.xml / hbase-default.xml
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testOptionalCacheFlushIntervalValue() {
    // 2. Prepare the test conditions: read the configuration value
    String key = "hbase.regionserver.optionalcacheflushinterval";
    int flushInterval = conf.getInt(key, 3600000);

    // 3. Test code: validate constraints
    // Constraint: must be an int >= 0
    assertTrue(
        "Configuration " + key + " must be an integer >= 0, but got: " + flushInterval,
        flushInterval >= 0);

    // 4. No special tear-down required for this test
  }
}