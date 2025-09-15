package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestHBaseMasterLoadBalancerClassConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseMasterLoadBalancerClassConfig.class);

  @Test
  public void testLoadBalancerClassNameIsValid() {
    Configuration conf = new Configuration(false);
    // 1. Use hbase 2.2.2 API to obtain the configured value (no hard-coding).
    String configuredClassName = conf.get(HConstants.HBASE_MASTER_LOADBALANCER_CLASS);

    // 2. If the property is not set, the default is valid by definition.
    if (configuredClassName == null) {
      return;
    }

    // 3. Validate that the value is a real class that implements LoadBalancer.
    try {
      Class<?> clazz = Class.forName(configuredClassName);
      if (!BaseLoadBalancer.class.isAssignableFrom(clazz)) {
        fail("Configured value for " + HConstants.HBASE_MASTER_LOADBALANCER_CLASS +
             " does not implement LoadBalancer: " + configuredClassName);
      }
    } catch (ClassNotFoundException e) {
      fail("Configured value for " + HConstants.HBASE_MASTER_LOADBALANCER_CLASS +
           " refers to a non-existent class: " + configuredClassName);
    }
  }
}