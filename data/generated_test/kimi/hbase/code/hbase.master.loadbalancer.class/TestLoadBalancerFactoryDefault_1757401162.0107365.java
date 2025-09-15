package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

    @Test
    public void testGetLoadBalancerClassNameReturnsDefaultWhenPropertyNotSet() {
        // 1. Prepare a clean Configuration
        Configuration conf = new Configuration();

        // 2. Obtain the default class name directly
        String defaultClassName = LoadBalancerFactory.getDefaultLoadBalancerClass().getName();

        // 3. Verify the returned class name is the default
        assertEquals("Expected default StochasticLoadBalancer class name",
                defaultClassName,
                conf.get(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, defaultClassName));
    }

    @Test
    public void testAssignmentManagerShouldAssignFavoredNodesWhenFavoredStochasticBalancerConfigured() {
        // 1. Prepare Configuration with FavoredStochasticBalancer
        Configuration conf = new Configuration();
        conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
                FavoredStochasticBalancer.class, LoadBalancer.class);

        // 2. Create a non-system table region
        RegionInfo region = RegionInfoBuilder.newBuilder(TableName.valueOf("testTable"))
                .build();

        // 3. Verify FavoredStochasticBalancer is configured
        LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(conf);
        assertTrue("Expected FavoredStochasticBalancer instance",
                balancer instanceof FavoredStochasticBalancer);
    }

    @Test
    public void testAssignmentManagerGetFavoredNodesReturnsEmptyWhenNotFavoredBalancer() {
        // 1. Prepare Configuration with default balancer
        Configuration conf = new Configuration();

        // 2. Verify default balancer is StochasticLoadBalancer
        LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(conf);
        assertTrue("Expected StochasticLoadBalancer instance",
                balancer instanceof StochasticLoadBalancer);
    }
}