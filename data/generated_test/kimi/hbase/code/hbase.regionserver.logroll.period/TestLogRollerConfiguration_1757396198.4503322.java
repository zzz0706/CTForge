package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestLogRollerConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogRollerConfiguration.class);

  @Test
  public void LogRollerUsesDefaultRollPeriodWhenNotConfigured() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    long expectedRollPeriod = conf.getLong("hbase.regionserver.logroll.period", 3600000);

    // 2. Prepare the test conditions.
    Server server = mock(Server.class);
    RegionServerServices services = mock(RegionServerServices.class);
    when(server.getConfiguration()).thenReturn(conf);

    // 3. Test code.
    LogRoller logRoller = new LogRoller(server, services);

    // 4. Code after testing.
    java.lang.reflect.Field rollPeriodField = LogRoller.class.getDeclaredField("rollPeriod");
    rollPeriodField.setAccessible(true);
    long actualRollPeriod = (Long) rollPeriodField.get(logRoller);
    assertEquals(expectedRollPeriod, actualRollPeriod);
  }
}