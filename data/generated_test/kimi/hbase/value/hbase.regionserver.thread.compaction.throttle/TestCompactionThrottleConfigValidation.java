package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({SmallTests.class, RegionServerTests.class})
public class TestCompactionThrottleConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionThrottleConfigValidation.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUp() {
    conf = HBaseConfiguration.create();
  }

  /**
   * Validates that the value of hbase.regionserver.thread.compaction.throttle
   * is at least 2 * hbase.hstore.compaction.max * hbase.hregion.memstore.flush.size
   * when the user has overridden it.
   */
  @Test
  public void testThrottleValueIsNotLessThanDerivedDefault() {
    long throttle = conf.getLong("hbase.regionserver.thread.compaction.throttle", -1L);

    // Skip test if the throttle is not explicitly set (i.e., using default)
    if (throttle == -1L) {
      return;
    }

    long maxFilesToCompact = conf.getInt("hbase.hstore.compaction.max", 10);
    long memStoreFlushSize = conf.getLong("hbase.hregion.memstore.flush.size", 134217728L);

    long derivedDefault = 2L * maxFilesToCompact * memStoreFlushSize;

    assertTrue(
        "Configured hbase.regionserver.thread.compaction.throttle (" + throttle
            + ") is less than the derived default value (" + derivedDefault + ")",
        throttle >= derivedDefault);
  }

  /**
   * Validates that the throttle value is a positive long.
   */
  @Test
  public void testThrottleValueIsPositiveLong() {
    long throttle = conf.getLong("hbase.regionserver.thread.compaction.throttle", -1L);
    if (throttle != -1L) {
      assertTrue("hbase.regionserver.thread.compaction.throttle must be a positive long",
          throttle > 0);
    }
  }
}