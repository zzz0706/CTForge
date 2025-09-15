package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class, MiscTests.class, RegionServerTests.class})
public class TestMobCacheEvictPeriodConfig {

  private static Configuration conf;

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule classRule =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestMobCacheEvictPeriodConfig.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = HBaseConfiguration.create();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    conf.clear();
  }

  @Test
  public void testMobCacheEvictPeriodPositive() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    long period = conf.getLong(MobConstants.MOB_CACHE_EVICT_PERIOD,
                               MobConstants.DEFAULT_MOB_CACHE_EVICT_PERIOD);

    // 2. Prepare the test conditions.
    // (Nothing to prepare; default is already present)

    // 3. Test code.
    // period must be > 0 (a non-positive period would cause the eviction thread to run continuously)
    if (period <= 0) {
      fail("Configuration " + MobConstants.MOB_CACHE_EVICT_PERIOD +
           " must be a positive integer, but found: " + period);
    }
  }

  @Test
  public void testMobCacheEvictPeriodType() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    String raw = conf.get(MobConstants.MOB_CACHE_EVICT_PERIOD);

    // 2. Prepare the test conditions.
    // raw can be null, so guard against NPE

    // 3. Test code.
    if (raw != null) {
      try {
        Long.parseLong(raw.trim());
      } catch (NumberFormatException e) {
        fail("Configuration " + MobConstants.MOB_CACHE_EVICT_PERIOD +
             " must be a valid long integer, but found: " + raw);
      }
    }
  }
}