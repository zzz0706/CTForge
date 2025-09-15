package org.apache.hadoop.hbase.master.balancer;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hbase.HBaseClassTestRule;       
import org.apache.hadoop.hbase.HBaseTestingUtility;       
import org.apache.hadoop.hbase.testclassification.MasterTests;       
import org.apache.hadoop.hbase.testclassification.SmallTests;       
import org.apache.hadoop.hbase.master.LoadBalancer;       
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;       
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;       
import org.junit.ClassRule;       
import org.junit.Test;       
import org.junit.experimental.categories.Category;       

import static org.junit.Assert.assertNotNull;       
import static org.junit.Assert.assertTrue;       

@Category({MasterTests.class, SmallTests.class})
public class TestLoadBalancerFactory {   

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE = 
        HBaseClassTestRule.forClass(TestLoadBalancerFactory.class);   

    private HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();   

    @Test 
    public void test_LoadBalancerFactory_GetLoadBalancer_ValidConfiguration() throws Exception {   
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.

        // Prepare the test conditions: Obtain the Configuration using HBaseTestingUtility.
        Configuration conf = hBaseTestingUtility.getConfiguration();

        // 2. Prepare the test conditions.
        // Ensure necessary settings for the LoadBalancerFactory are configured in 'conf'.
        conf.setClass("hbase.master.loadbalancer.class", 
            org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer.class, 
            org.apache.hadoop.hbase.master.LoadBalancer.class);

        // 3. Test code.
        // Obtain the LoadBalancer using the factory method.
        LoadBalancer loadBalancer = LoadBalancerFactory.getLoadBalancer(conf);

        // 4. Code after testing.
        // Assertions: Verify the instance is not null and matches the expected type.
        assertNotNull("LoadBalancer instance should not be null", loadBalancer);
        assertTrue("LoadBalancer instance should match the configured class",
            loadBalancer instanceof StochasticLoadBalancer);

        // Further verify the LoadBalancer's configuration and basic setup behavior.
        loadBalancer.setConf(conf);
        assertNotNull("LoadBalancer configuration should be set successfully", loadBalancer.getConf());
    }   
}