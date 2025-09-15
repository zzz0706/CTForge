package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMemstoreLowerLimitConfigValidation {

  private static Configuration conf;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMemstoreLowerLimitConfigValidation.class);

  @BeforeClass
  public static void setUp() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testLowerLimitIsValidFloatInRangeZeroToOne() {
    // 2. Prepare the test conditions.
    String val = conf.get(MemorySizeUtil.MEMSTORE_SIZE_LOWER_LIMIT_KEY);
    if (val != null) {
      try {
        float f = Float.parseFloat(val);
        // 3. Test code.
        assertTrue("hbase.regionserver.global.memstore.size.lower.limit must be <= 1.0", f <= 1.0f);
        assertTrue("hbase.regionserver.global.memstore.size.lower.limit must be >= 0.0", f >= 0.0f);
      } catch (NumberFormatException nfe) {
        assertTrue("hbase.regionserver.global.memstore.size.lower.limit is not a valid float", false);
      }
    }
    // 4. Code after testing.
  }

  @Test
  public void testLowerLimitNotGreaterThanUpperLimit() {
    // 2. Prepare the test conditions.
    String lower = conf.get(MemorySizeUtil.MEMSTORE_SIZE_LOWER_LIMIT_KEY);
    if (lower != null) {
      try {
        float lowerF = Float.parseFloat(lower);
        float upperF = MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false);
        // 3. Test code.
        assertTrue("lower limit must be <= upper limit",
                   lowerF <= upperF);
      } catch (NumberFormatException nfe) {
        assertTrue("hbase.regionserver.global.memstore.size.lower.limit is not a valid float", false);
      }
    }
    // 4. Code after testing.
  }
}