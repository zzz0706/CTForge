package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestMobFileCacheClampOverOneRemainRatio {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobFileCacheClampOverOneRemainRatio.class);

  @Test
  public void testClampOverOneRemainRatio() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions.
    float expectedRemainRatio = 1.0f;

    // 3. Test code.
    // Enable cache by setting a positive size
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 10);
    // Set ratio above 1.0
    conf.setFloat(MobConstants.MOB_CACHE_EVICT_REMAIN_RATIO, 1.5f);

    // 4. Code after testing.
    MobFileCache cache = new MobFileCache(conf);

    Field field = MobFileCache.class.getDeclaredField("evictRemainRatio");
    field.setAccessible(true);
    float actualRemainRatio = (float) field.get(cache);
    assertEquals(expectedRemainRatio, actualRemainRatio, 0.0001f);
  }
}