package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestMobUtilsConfiguration {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestMobUtilsConfiguration.class);

  @Test
  public void testCreateMobCompactorThreadPoolRespectsCustomPositiveValue() {
    // 1. Instantiate Configuration and set the custom value
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(MobConstants.MOB_COMPACTION_THREADS_MAX, 5);

    // 2. Compute expected value (the custom value itself)
    int expectedMaxPoolSize = conf.getInt(MobConstants.MOB_COMPACTION_THREADS_MAX,
                                          MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);

    // 3. Invoke the method under test
    ExecutorService executor = MobUtils.createMobCompactorThreadPool(conf);

    // 4. Assertions
    assertEquals(expectedMaxPoolSize, ((ThreadPoolExecutor) executor).getMaximumPoolSize());
  }
}