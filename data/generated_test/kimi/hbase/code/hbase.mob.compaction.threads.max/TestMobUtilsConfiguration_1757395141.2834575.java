package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterMobCompactionThread;
import org.apache.hadoop.hbase.master.MobCompactionChore;
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
    ExecutorService executor = MobUtils.createMobCompactorThreadPool(conf);
    ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;

    // 4. Assert the result
    assertEquals(expectedMaxThreads, pool.getMaximumPoolSize());
  }

  @Test
  public void testCreateMobCompactorThreadPoolUsesConfiguredValue() {
    // 1. Create a Configuration and set a custom value
    Configuration conf = new Configuration();
    int customThreads = 5;
    conf.setInt(MobConstants.MOB_COMPACTION_THREADS_MAX, customThreads);

    // 2. Invoke the method under test
    ExecutorService executor = MobUtils.createMobCompactorThreadPool(conf);
    ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;

    // 3. Assert the result
    assertEquals(customThreads, pool.getMaximumPoolSize());
  }

  @Test
  public void testMasterMobCompactionThreadReadsConfiguration() throws Exception {
    // 1. Prepare a mock HMaster with a Configuration
    Configuration conf = new Configuration();
    conf.setInt(MobConstants.MOB_COMPACTION_THREADS_MAX, 3);
    HMaster master = mock(HMaster.class);
    when(master.getConfiguration()).thenReturn(conf);

    // 2. Create the object under test
    MasterMobCompactionThread thread = new MasterMobCompactionThread(master);

    // 3. Assert the result via reflection
    java.lang.reflect.Field field = MasterMobCompactionThread.class.getDeclaredField("mobCompactorPool");
    field.setAccessible(true);
    ExecutorService executor = (ExecutorService) field.get(thread);
    assertNotNull(executor);
    assertEquals(3, ((ThreadPoolExecutor) executor).getMaximumPoolSize());
  }

  @Test
  public void testMobCompactionChoreReadsConfiguration() throws Exception {
    // 1. Prepare a mock HMaster with a Configuration
    Configuration conf = new Configuration();
    conf.setInt(MobConstants.MOB_COMPACTION_THREADS_MAX, 4);
    HMaster master = mock(HMaster.class);
    when(master.getConfiguration()).thenReturn(conf);
    when(master.getServerName()).thenReturn(ServerName.valueOf("test-server", 16000, 123456789L));

    // 2. Create the object under test
    MobCompactionChore chore = new MobCompactionChore(master, 60);

    // 3. Assert the result via reflection
    java.lang.reflect.Field field = MobCompactionChore.class.getDeclaredField("pool");
    field.setAccessible(true);
    ExecutorService executor = (ExecutorService) field.get(chore);
    assertNotNull(executor);
    assertEquals(4, ((ThreadPoolExecutor) executor).getMaximumPoolSize());
  }
}