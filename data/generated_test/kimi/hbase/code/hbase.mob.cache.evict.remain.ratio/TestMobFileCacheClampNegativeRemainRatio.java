package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({MediumTests.class})
public class TestMobFileCacheClampNegativeRemainRatio {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobFileCacheClampNegativeRemainRatio.class);

  @Test
  public void testClampNegativeRemainRatio() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 10); // enable cache
    conf.setFloat(MobConstants.MOB_CACHE_EVICT_REMAIN_RATIO, -0.3f);

    // 2. Dynamic expected value calculation
    float expectedRemainRatio = 0.0f;

    // 3. Prepare the test conditions
    // No external mocking needed for this test

    // 4. Test code
    MobFileCache cache = new MobFileCache(conf);

    // 5. Code after testing
    Field evictRemainRatioField = MobFileCache.class.getDeclaredField("evictRemainRatio");
    evictRemainRatioField.setAccessible(true);
    float actualRemainRatio = (float) evictRemainRatioField.get(cache);

    assertEquals(expectedRemainRatio, actualRemainRatio, 0.0001f);
  }
}