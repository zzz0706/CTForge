package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit test for LoadBalancerFactory.
 * 
 * This test checks that the default LoadBalancer instance is properly obtained from the factory and initialized.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestLoadBalancerFactory {

    @ClassRule // HBaseClassTestRule for managing test execution
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestLoadBalancerFactory.class);

    @Test
    public void testLoadBalancerFactoryGetLoadBalancerDefaultConfiguration() {
        // 1. Prepare the test conditions:
        // Use HBaseConfiguration to get a default Configuration object.
        Configuration configuration = HBaseConfiguration.create();

        // 2. Test the code:
        // Call LoadBalancerFactory.getLoadBalancer and retrieve the default LoadBalancer instance.
        LoadBalancer loadBalancer = LoadBalancerFactory.getLoadBalancer(configuration);

        // Define the default LoadBalancer class manually, as the API no longer provides this functionality.
        // The default LoadBalancer class is `org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer`.
        Class<? extends LoadBalancer> defaultLoadBalancerClass = org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer.class;

        // Assert that the returned LoadBalancer instance matches the expected default LoadBalancer class.
        assert loadBalancer != null : "LoadBalancer instance should not be null.";
        assert loadBalancer.getClass().equals(defaultLoadBalancerClass) :
            "Returned LoadBalancer instance does not match the expected default LoadBalancer class.";

        // 3. Verify the functionality of the loaded instance:
        try {
            // Attempt to initialize the load balancer to ensure proper functionality.
            if (loadBalancer instanceof BaseLoadBalancer) {
                ((BaseLoadBalancer) loadBalancer).setConf(configuration);
                ((BaseLoadBalancer) loadBalancer).initialize();
                System.out.println("LoadBalancer default instance initialized successfully.");
            } else {
                throw new RuntimeException("LoadBalancer instance is not of type BaseLoadBalancer.");
            }
        } catch (Exception e) {
            assert false : "Default LoadBalancer instance failed to initialize: " + e.getMessage();
        }

        // 4. Additional validation to ensure no runtime issues.
        System.out.println("TestLoadBalancerFactory test completed successfully.");
    }
}