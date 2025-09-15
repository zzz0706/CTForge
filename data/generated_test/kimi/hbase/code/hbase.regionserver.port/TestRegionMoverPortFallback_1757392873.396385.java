package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestRegionMoverPortFallback {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionMoverPortFallback.class);

  @Test
  public void verifyRegionMoverUsesConfiguredPortWhenHostnameHasNoPort() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = HBaseConfiguration.create();

    // 2. Prepare the test conditions.
    int expectedPort = 22222;
    conf.setInt(HConstants.REGIONSERVER_PORT, expectedPort);

    // 3. Test code.
    RegionMover.RegionMoverBuilder builder =
        new RegionMover.RegionMoverBuilder("rs1.example.com", conf);

    // 4. Code after testing.
    assertEquals(expectedPort, builder.port);
  }
}