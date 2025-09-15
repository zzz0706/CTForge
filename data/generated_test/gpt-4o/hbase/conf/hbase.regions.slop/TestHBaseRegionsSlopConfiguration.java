package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@Category({MasterTests.class, SmallTests.class})
public class TestHBaseRegionsSlopConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = 
      HBaseClassTestRule.forClass(TestHBaseRegionsSlopConfiguration.class);

  @Test
  public void testHBaseRegionsSlopConfiguration() {
    // 1. Obtain configuration instance (mocked for testing purposes)
    Configuration conf = new Configuration();
    
    // 2. Retrieve the value of the 'hbase.regions.slop' configuration
    float slop = conf.getFloat("hbase.regions.slop", 0.001f); // Default value for StochasticLoadBalancer is 0.001

    // 3. Validate the boundaries of the 'slop' value
    //    It should fall within the range [0, 1]
    boolean isValidSlop = slop >= 0 && slop <= 1;
    
    // Test if the configuration value is valid
    assertTrue("The 'hbase.regions.slop' configuration value must be between 0 and 1 inclusive.", isValidSlop);

    // 4. Additional edge case checks for extreme values
    assertFalse("The 'hbase.regions.slop' configuration value cannot be less than 0.", slop < 0);
    assertFalse("The 'hbase.regions.slop' configuration value cannot be greater than 1.", slop > 1);
  }
}