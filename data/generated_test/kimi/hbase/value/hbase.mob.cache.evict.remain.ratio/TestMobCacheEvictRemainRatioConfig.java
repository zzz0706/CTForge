package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobCacheEvictRemainRatioConfig {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule classRule =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestMobCacheEvictRemainRatioConfig.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUp() throws Exception {
    // 1. Use the hbase 2.2.2 API to obtain configuration values
    conf = HBaseConfiguration.create();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // 4. Code after testing
    conf = null;
  }

  @Test
  public void testMobCacheEvictRemainRatioValidRange() {
    // 2. Prepare test conditions – read the configuration value without setting it in test code
    float ratio = conf.getFloat(MobConstants.MOB_CACHE_EVICT_REMAIN_RATIO,
        MobConstants.DEFAULT_EVICT_REMAIN_RATIO);

    // 3. Test code – verify the value is within the allowed range [0.0, 1.0]
    if (ratio < 0.0f || ratio > 1.0f) {
      fail("Configuration hbase.mob.cache.evict.remain.ratio must be between 0.0 and 1.0 inclusive, but was: " + ratio);
    }
    assertTrue("hbase.mob.cache.evict.remain.ratio is within valid range", ratio >= 0.0f && ratio <= 1.0f);
  }
}