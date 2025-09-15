package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, SmallTests.class})
public class TestBaseLoadBalancerDefaultSlop {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestBaseLoadBalancerDefaultSlop.class);

    @Test
    public void testDefaultSlopValueIsLoadedFromConfiguration() throws Exception {
        // 1. Create a fresh Configuration instance without overrides
        Configuration conf = new Configuration();

        // 2. Compute the expected default value dynamically
        float expectedSlop = conf.getFloat("hbase.regions.slop", 0.2f);

        // 3. Instantiate BaseLoadBalancer via reflection (since it's abstract)
        BaseLoadBalancer balancer = new SimpleLoadBalancer();
        balancer.setConf(conf);

        // 4. Read the slop field via reflection
        Field slopField = BaseLoadBalancer.class.getDeclaredField("slop");
        slopField.setAccessible(true);
        float actualSlop = (Float) slopField.get(balancer);

        // 5. Assert the loaded value matches the expected default
        assertEquals("Default slop value should match Configuration default", expectedSlop, actualSlop, 0.0001f);
    }

    /**
     * A minimal concrete subclass of BaseLoadBalancer so we can instantiate it.
     */
    private static class SimpleLoadBalancer extends BaseLoadBalancer {
        // empty
    }
}