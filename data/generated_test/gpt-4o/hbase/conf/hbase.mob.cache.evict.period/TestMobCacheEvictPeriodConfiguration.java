package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestMobCacheEvictPeriodConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobCacheEvictPeriodConfiguration.class);

  private Configuration conf;

  @Before
  public void setUp() {
    // Initialize a configuration with default values for HBase testing.
    conf = new Configuration(false);
  }

  /**
   * Test to ensure the hbase.mob.cache.evict.period configuration value is valid
   * and satisfies the constraints.
   */
  @Test
  public void testMobCacheEvictPeriodConfigurationValidity() {
    // Retrieve the value from the configuration
    long mobCacheEvictPeriod = conf.getLong(
        MobConstants.MOB_CACHE_EVICT_PERIOD, 
        MobConstants.DEFAULT_MOB_CACHE_EVICT_PERIOD
    );

    // Validate the value: it should be a positive integer.
    assertTrue(
        "hbase.mob.cache.evict.period must be greater than 0",
        mobCacheEvictPeriod > 0
    );
  }

  /** 
   * Test to ensure dependencies of hbase.mob.cache.evict.period are valid.
   */
  @Test
  public void testMobCacheEvictPeriodDependencyConstraints() {
    // Retrieve potential dependent configuration values
    float evictRemainRatio = conf.getFloat(
        MobConstants.MOB_CACHE_EVICT_REMAIN_RATIO, 
        MobConstants.DEFAULT_EVICT_REMAIN_RATIO
    );

    // Validate the dependency: evictRemainRatio must be within [0.0, 1.0]
    assertTrue(
        "hbase.mob.cache.evict.remain.ratio must be between 0.0 and 1.0",
        evictRemainRatio >= 0.0f && evictRemainRatio <= 1.0f
    );
  }
}