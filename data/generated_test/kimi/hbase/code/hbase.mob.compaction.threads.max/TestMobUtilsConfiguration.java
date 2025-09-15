package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobUtilsConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobUtilsConfiguration.class);

  @Test
  public void testCreateMobCompactorThreadPoolUsesDefaultWhenUnset() {
    // 1. Create a fresh Configuration instance without setting the key
    Configuration conf = new Configuration();

    // 2. Compute the expected default value dynamically
    int expectedMaxThreads = conf.getInt(MobConstants.MOB_COMPACTION_THREADS_MAX,
                                        MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);
    if (expectedMaxThreads == 0) {
      expectedMaxThreads = 1;
    }

    // 3. Invoke the method under test
    java.util.concurrent.ExecutorService executor = MobUtils.createMobCompactorThreadPool(conf);
    java.util.concurrent.ThreadPoolExecutor pool = (java.util.concurrent.ThreadPoolExecutor) executor;

    // 4. Assert the result
    assertEquals(expectedMaxThreads, pool.getMaximumPoolSize());
  }
}