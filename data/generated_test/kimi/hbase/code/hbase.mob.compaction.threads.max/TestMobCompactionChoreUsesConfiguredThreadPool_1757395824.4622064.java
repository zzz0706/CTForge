package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

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
    HMaster mockMaster = mock(HMaster.class);
    when(mockMaster.getConfiguration()).thenReturn(conf);

    // 3. Test code.
    MobCompactionChore chore = new MobCompactionChore(mockMaster, 60);

    // 4. Code after testing.
    // Access the private field 'pool' via reflection
    Field poolField = MobCompactionChore.class.getDeclaredField("pool");
    poolField.setAccessible(true);
    Object pool = poolField.get(chore);

    // Verify the pool has the correct max pool size
    assertEquals(3, pool instanceof java.util.concurrent.ThreadPoolExecutor
        ? ((java.util.concurrent.ThreadPoolExecutor) pool).getMaximumPoolSize()
        : -1);
  }
}