package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHBaseMasterMobTtlCleanerPeriodConfig {

  private static Configuration conf;

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHBaseMasterMobTtlCleanerPeriodConfig.class);

  @BeforeClass
  public static void setUpBeforeClass() {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testMobCleanerPeriodValid() {
    int period = conf.getInt(MobConstants.MOB_CLEANER_PERIOD,
                             MobConstants.DEFAULT_MOB_CLEANER_PERIOD);

    // period must be positive (> 0)
    assertTrue("hbase.master.mob.ttl.cleaner.period must be > 0 seconds",
               period > 0);

    // period must be an integer (already enforced by getInt)
    assertTrue("hbase.master.mob.ttl.cleaner.period must be an integer",
               period == (int) period);
  }
}