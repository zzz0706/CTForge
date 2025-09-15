package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestHbaseRegionsSlopConfig {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHbaseRegionsSlopConfig.class);

  @Test
  public void testHbaseRegionsSlopValue() {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource("hbase-site.xml");

    float slop = conf.getFloat("hbase.regions.slop", 0.2f);

    // Constraint: 0 <= slop <= 1
    if (slop < 0 || slop > 1) {
      fail("Invalid value for hbase.regions.slop: " + slop +
           ". Expected value between 0 and 1 (inclusive).");
    }

    // Additional check: ensure the value is a valid float
    try {
      Float.parseFloat(String.valueOf(slop));
    } catch (NumberFormatException e) {
      fail("Invalid format for hbase.regions.slop: " + slop);
    }

    assertTrue("hbase.regions.slop is within valid range [0,1]", slop >= 0 && slop <= 1);
  }
}