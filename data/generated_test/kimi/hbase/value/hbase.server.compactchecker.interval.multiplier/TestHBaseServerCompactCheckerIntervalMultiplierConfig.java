package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHBaseServerCompactCheckerIntervalMultiplierConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseServerCompactCheckerIntervalMultiplierConfig.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUp() {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testCompactCheckerIntervalMultiplierIsPositive() {
    int multiplier = conf.getInt("hbase.server.compactchecker.interval.multiplier", 1000);
    assertTrue("hbase.server.compactchecker.interval.multiplier must be > 0", multiplier > 0);
  }

  @Test
  public void testCompactCheckerIntervalMultiplierIsInteger() {
    String val = conf.get("hbase.server.compactchecker.interval.multiplier");
    if (val != null) {
      try {
        Integer.parseInt(val);
      } catch (NumberFormatException e) {
        assertTrue("hbase.server.compactchecker.interval.multiplier must be a valid integer", false);
      }
    }
  }
}