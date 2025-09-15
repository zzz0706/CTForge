package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestExpiredMobFileCleanerChoreConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExpiredMobFileCleanerChoreConfig.class);

  @Test
  public void verifyMissedStartTimeThresholdUsesConfiguredPeriod() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();
    conf.setInt(MobConstants.MOB_CLEANER_PERIOD, 3600);

    // 2. Prepare the test conditions
    HMaster mockMaster = mock(HMaster.class);
    when(mockMaster.getConfiguration()).thenReturn(conf);
    when(mockMaster.getServerName()).thenReturn(
        org.apache.hadoop.hbase.ServerName.valueOf("mock-server,12345,123456789"));
    when(mockMaster.getChoreService()).thenReturn(null);
    when(mockMaster.isStopped()).thenReturn(false);
    when(mockMaster.isAborted()).thenReturn(false);

    // 3. Test code
    ExpiredMobFileCleanerChore chore = new ExpiredMobFileCleanerChore(mockMaster);

    // Access private method via reflection
    java.lang.reflect.Method method =
        ScheduledChore.class.getDeclaredMethod("getMaximumAllowedTimeBetweenRuns");
    method.setAccessible(true);
    long actualThreshold = (long) (double) method.invoke(chore);
    long expectedThreshold = (long) (1.5 * 3600 * 1000);

    // 4. Code after testing
    assertEquals(expectedThreshold, actualThreshold);
  }
}