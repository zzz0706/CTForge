package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestLoadBalancerFactoryDefault {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestLoadBalancerFactoryDefault.class);

    @Test
    public void testDefaultLoadBalancerIsUsedWhenPropertyNotSet() {
        // 1. Create a new Configuration object without setting hbase.master.loadbalancer.class
        Configuration conf = new Configuration();

        // 2. Call LoadBalancerFactory.getLoadBalancer(conf) to obtain the balancer instance
        LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(conf);

        // 3. Assert that the returned instance is of type StochasticLoadBalancer
        assertTrue("Expected StochasticLoadBalancer instance",
                balancer instanceof StochasticLoadBalancer);
    }
}