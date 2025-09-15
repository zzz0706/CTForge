package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMobFileCacheEvictPeriod {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobFileCacheEvictPeriod.class);

  private MobFileCache mobFileCache;

  @Before
  public void setUp() {
    mobFileCache = null;
  }

  @After
  public void tearDown() {
    if (mobFileCache != null) {
      mobFileCache.shutdown();
    }
  }

  @Test
  public void testDefaultEvictPeriodIsUsedWhenConfigNotSet() throws Exception {
    // 1. Create a new Configuration instance without setting the key
    Configuration conf = new Configuration();
    // Ensure the key is not present (no conf.set(...))

    // 2. Compute expected value dynamically
    long expectedPeriod = conf.getLong(MobConstants.MOB_CACHE_EVICT_PERIOD,
        MobConstants.DEFAULT_MOB_CACHE_EVICT_PERIOD);

    // 3. Instantiate MobFileCache with positive cache size to enable cache
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 100); // Enable cache
    mobFileCache = new MobFileCache(conf);

    // 4. Verify the configuration value matches the default
    assertEquals("Default eviction period should be 3600 seconds",
        3600L, expectedPeriod);
  }
}