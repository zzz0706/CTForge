package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({MasterTests.class, SmallTests.class})
public class TestBaseLoadBalancerSlopClamping {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBaseLoadBalancerSlopClamping.class);

  @Test
  public void testSlopClampingToOneWhenGreaterThanOne() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat("hbase.regions.slop", 1.5f);

    // 2. Prepare the test conditions.
    // BaseLoadBalancer is abstract; use a concrete subclass such as SimpleLoadBalancer
    BaseLoadBalancer balancer = new SimpleLoadBalancer();
    balancer.setConf(conf);

    // 3. Test code.
    Field slopField = BaseLoadBalancer.class.getDeclaredField("slop");
    slopField.setAccessible(true);
    float actualSlop = slopField.getFloat(balancer);

    // 4. Code after testing.
    assertEquals(1.0f, actualSlop, 0.0001f);
  }
}