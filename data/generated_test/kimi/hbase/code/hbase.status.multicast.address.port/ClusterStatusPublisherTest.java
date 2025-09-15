package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class ClusterStatusPublisherTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(ClusterStatusPublisherTest.class);

  @Test
  public void testCustomPortIsUsedWhenConfigured() {
    Configuration conf = new Configuration();
    int customPort = 19999;
    conf.setInt(HConstants.STATUS_MULTICAST_PORT, customPort);

    int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
        HConstants.DEFAULT_STATUS_MULTICAST_PORT);
    assertEquals(customPort, port);
  }
}