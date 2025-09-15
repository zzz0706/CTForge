package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({RegionServerTests.class, SmallTests.class})
public class TestLogRollerRecomputesRollPeriodOnRestart {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogRollerRecomputesRollPeriodOnRestart.class);

  @Test
  public void testLogRollerRecomputesRollPeriodOnRestart() throws Exception {
    // 1. Configuration as input
    Configuration conf = HBaseConfiguration.create();

    // 2. Dynamic expected value calculation
    long expectedInitial = conf.getLong("hbase.regionserver.logroll.period", 3600000);
    long expectedUpdated = expectedInitial + 1000; // simulate update

    // 3. Prepare the test conditions
    Server server1 = mock(Server.class);
    when(server1.getConfiguration()).thenReturn(conf);

    RegionServerServices services = mock(RegionServerServices.class);

    // 4. Test code
    LogRoller roller1 = new LogRoller(server1, services);
    Field rollPeriodField = LogRoller.class.getDeclaredField("rollPeriod");
    rollPeriodField.setAccessible(true);
    assertEquals("Initial rollPeriod should match configuration", expectedInitial, rollPeriodField.getLong(roller1));

    // Stop LogRoller thread (simulated by not starting it)
    // Update configuration
    conf.setLong("hbase.regionserver.logroll.period", expectedUpdated);

    // Create new instance with updated configuration
    Server server2 = mock(Server.class);
    when(server2.getConfiguration()).thenReturn(conf);

    LogRoller roller2 = new LogRoller(server2, services);
    assertEquals("Updated rollPeriod should match new configuration", expectedUpdated, rollPeriodField.getLong(roller2));
  }
}