package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHBaseMemStoreLowerLimitConfig {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHBaseMemStoreLowerLimitConfig.class);

  @Test
  public void verifyDeprecatedKeyFallback() {
    // 1. Configuration as Input
    Configuration conf = HBaseConfiguration.create();
    // Set the deprecated key to 0.4 (40%)
    conf.set("hbase.regionserver.global.memstore.lowerLimit", "0.4");

    // 2. Dynamic Expected Value Calculation
    float upper = MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false);
    float expected = 0.4f / upper;

    // 3. Prepare the test conditions
    // No external dependencies to mock in this test case

    // 4. Test code
    float actual = MemorySizeUtil.getGlobalMemStoreHeapLowerMark(conf, true);

    // 5. Code after testing
    assertEquals(expected, actual, 0.0001);
  }
}