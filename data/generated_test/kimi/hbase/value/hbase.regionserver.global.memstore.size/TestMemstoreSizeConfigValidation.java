package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMemstoreSizeConfigValidation {

  private static Configuration conf;

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestMemstoreSizeConfigValidation.class);

  @BeforeClass
  public static void setUp() {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testGlobalMemstoreSizeWithinRange() {
    // 1. Obtain the value via the official API
    float limit = MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false);

    // 2. Validate range (0, 0.8]
    if (limit <= 0.0f || limit > 0.8f) {
      fail("hbase.regionserver.global.memstore.size must be in the range (0 -> 0.8], but was: "
          + limit);
    }
  }

  @Test
  public void testGlobalMemstoreSizeLowerLimitWithinRange() {
    // 1. Obtain the value via the official API
    float lowerMark = MemorySizeUtil.getGlobalMemStoreHeapLowerMark(conf, true);

    // 2. Validate range (0, 1.0]
    if (lowerMark <= 0.0f || lowerMark > 1.0f) {
      fail("hbase.regionserver.global.memstore.size.lower.limit must be in the range (0 -> 1.0], but was: "
          + lowerMark);
    }
  }

  @Test
  public void testGlobalMemstoreSizeLowerLimitNotGreaterThanUpper() {
    // 1. Obtain both values via the official API
    float upper = MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false);
    float lowerRatio = MemorySizeUtil.getGlobalMemStoreHeapLowerMark(conf, true);

    // 2. Ensure lower limit (expressed as ratio of upper) is not > 1.0
    if (lowerRatio > 1.0f) {
      fail("hbase.regionserver.global.memstore.size.lower.limit ratio must not exceed 1.0, but was: "
          + lowerRatio);
    }
  }
}