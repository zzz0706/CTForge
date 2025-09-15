package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
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
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestExpiredMobFileCleanerChoreConfig.class);

  @Test
  public void verifyMissedStartTimeThresholdUsesConfiguredPeriod() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();

    // 2. Dynamic expected value calculation
    int configuredPeriod = conf.getInt(
        MobConstants.MOB_CLEANER_PERIOD,
        MobConstants.DEFAULT_MOB_CLEANER_PERIOD);
    long expectedThreshold = (long) (1.5 * configuredPeriod * 1000);

    // 3. Mock/stub external dependencies
    HMaster mockMaster = mock(HMaster.class);
    when(mockMaster.getConfiguration()).thenReturn(conf);
    when(mockMaster.getServerName()).thenReturn(org.apache.hadoop.hbase.ServerName.valueOf("mock-server,12345,123456789"));
    when(mockMaster.getChoreService()).thenReturn(null);
    when(mockMaster.isStopped()).thenReturn(false);
    when(mockMaster.isAborted()).thenReturn(false);

    // 4. Invoke the method under test
    ExpiredMobFileCleanerChore chore = new ExpiredMobFileCleanerChore(mockMaster);

    // 5. Assertions and verification
    // Access private method via reflection
    java.lang.reflect.Method method =
        chore.getClass().getSuperclass().getDeclaredMethod("getMaximumAllowedTimeBetweenRuns");
    method.setAccessible(true);
    double actualThreshold = (double) method.invoke(chore);
    assertEquals(expectedThreshold, (long) actualThreshold);
  }
}