package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 * Test class to verify the functionality of `getLoadBalancerClassName` method when the 
 * `hbase.master.loadbalancer.class` configuration is not explicitly set and default mechanism is used.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestHMaster {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestHMaster.class);

    private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @Test
    public void testGetLoadBalancerClassName_DefaultConfiguration() throws Exception {
        // Prepare the configuration and initialize test conditions
        TEST_UTIL.startMiniCluster(); // Starts a Mini HBase cluster for testing
        try {
            HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
            
            // Test: Retrieve the load balancer class name via getLoadBalancerClassName
            String loadBalancerClassName = master.getLoadBalancerClassName();
            
            // Verify: Ensure the retrieved class is equal to the default load balancer class
            assertEquals(LoadBalancerFactory.getDefaultLoadBalancerClass().getName(), loadBalancerClassName);
        } finally {
            // Cleanup after the test
            TEST_UTIL.shutdownMiniCluster();
        }
    }
}