package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestLogRollerRecomputesRollPeriodOnRestart {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogRollerRecomputesRollPeriodOnRestart.class);

  private Configuration conf;
  private Server server1;
  private Server server2;
  private RegionServerServices services;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    services = mock(RegionServerServices.class);
  }

  @Test
  public void testLogRollerRecomputesRollPeriodOnRestart() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    conf.setLong("hbase.regionserver.logroll.period", 1000L);
    long expectedInitial = conf.getLong("hbase.regionserver.logroll.period", 3600000L);

    // 2. Prepare the test conditions.
    server1 = mock(Server.class);
    when(server1.getConfiguration()).thenReturn(conf);

    // 3. Test code.
    LogRoller roller1 = new LogRoller(server1, services);
    Field rollPeriodField = LogRoller.class.getDeclaredField("rollPeriod");
    rollPeriodField.setAccessible(true);
    assertEquals("Initial rollPeriod should match configuration", expectedInitial, rollPeriodField.getLong(roller1));

    // Stop LogRoller thread (simulated by not starting it)
    roller1.interrupt();

    // Update configuration
    conf.setLong("hbase.regionserver.logroll.period", 2000L);
    long expectedUpdated = conf.getLong("hbase.regionserver.logroll.period", 3600000L);

    // Create new instance with updated configuration
    server2 = mock(Server.class);
    when(server2.getConfiguration()).thenReturn(conf);

    LogRoller roller2 = new LogRoller(server2, services);
    assertEquals("Updated rollPeriod should match new configuration", expectedUpdated, rollPeriodField.getLong(roller2));

    // 4. Code after testing.
    roller2.interrupt();
  }

  @After
  public void tearDown() {
    // Clean up resources if needed
  }
}