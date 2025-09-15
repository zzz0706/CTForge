package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.LoadBalancerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestLoadBalancerFactory {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLoadBalancerFactory.class);

  @Test
  public void testCustomLoadBalancerIsInstantiatedWhenPropertySet() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions.
    String expectedClassName = SimpleLoadBalancer.class.getName();
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, expectedClassName);

    // 3. Test code.
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(conf);

    // 4. Code after testing.
    assertTrue("Returned balancer should be instance of " + expectedClassName,
               balancer instanceof SimpleLoadBalancer);
  }
}