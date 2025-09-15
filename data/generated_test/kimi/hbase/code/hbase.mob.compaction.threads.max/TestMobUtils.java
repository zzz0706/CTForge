package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
public class TestMobUtils {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestMobUtils.class);

  @Test
  public void testCreateMobCompactorThreadPoolClampsZeroToOne() {
    // 1. Instantiate Configuration
    Configuration conf = HBaseConfiguration.create();

    // 2. Explicitly set the key to 0 to trigger the clamping logic
    conf.setInt(MobConstants.MOB_COMPACTION_THREADS_MAX, 0);

    // 3. Invoke the method under test
    ExecutorService pool = MobUtils.createMobCompactorThreadPool(conf);

    // 4. Compute expected value: 0 is clamped to 1
    long expectedMaxPoolSize = 1;

    // 5. Assert the actual value
    assertEquals(expectedMaxPoolSize, ((ThreadPoolExecutor) pool).getMaximumPoolSize());

    // 6. Clean-up
    pool.shutdownNow();
  }
}