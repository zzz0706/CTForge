package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestMobCompactionChoreUsesConfiguredThreadPool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobCompactionChoreUsesConfiguredThreadPool.class);

  @Test
  public void testMobCompactionChoreUsesConfiguredThreadPool() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    conf.setInt(MobConstants.MOB_COMPACTION_THREADS_MAX, 3);

    // 2. Prepare the test conditions.
    int expectedMaxThreads = conf.getInt(MobConstants.MOB_COMPACTION_THREADS_MAX,
        MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);
    if (expectedMaxThreads == 0) {
      expectedMaxThreads = 1;
    }

    HMaster mockMaster = mock(HMaster.class);
    when(mockMaster.getConfiguration()).thenReturn(conf);

    // 3. Test code.
    MobCompactionChore chore = new MobCompactionChore(mockMaster, 60);
    Field poolField = MobCompactionChore.class.getDeclaredField("pool");
    poolField.setAccessible(true);
    ExecutorService pool = (ExecutorService) poolField.get(chore);

    // 4. Code after testing.
    assertEquals(ThreadPoolExecutor.class, pool.getClass());
    ThreadPoolExecutor threadPool = (ThreadPoolExecutor) pool;
    assertEquals(expectedMaxThreads, threadPool.getMaximumPoolSize());
  }
}