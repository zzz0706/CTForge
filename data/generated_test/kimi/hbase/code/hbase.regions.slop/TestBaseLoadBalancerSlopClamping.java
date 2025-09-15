package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestBaseLoadBalancerSlopClamping {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBaseLoadBalancerSlopClamping.class);

  @Test
  public void testSlopClampingToZeroWhenNegative() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();
    conf.setFloat("hbase.regions.slop", -0.5f);

    // 2. Dynamic expected value calculation
    float expectedSlop = 0.0f;

    // 3. Mock/stub external dependencies – none required for this test

    // 4. Invoke the method under test
    // BaseLoadBalancer is abstract in 2.2.2, use a concrete subclass
    StochasticLoadBalancer balancer = new StochasticLoadBalancer();
    balancer.setConf(conf);

    // 5. Assertions and verification
    java.lang.reflect.Field slopField = BaseLoadBalancer.class.getDeclaredField("slop");
    slopField.setAccessible(true);
    float actualSlop = (Float) slopField.get(balancer);
    assertEquals(expectedSlop, actualSlop, 0.0001f);
  }
}