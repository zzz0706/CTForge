package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Unit test for validating the configuration of hbase.master.loadbalancer.class
 * This test ensures that the configured class implements LoadBalancer interface
 * and matches the default when not explicitly set.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestLoadBalancerConfigurationValidation {
  
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLoadBalancerConfigurationValidation.class);

  @Test
  public void testLoadBalancerClassConfiguration() {
    // Load HBase configuration
    Configuration conf = HBaseConfiguration.create();

    // Retrieve configured load balancer class name (with default fallback)
    String key = HConstants.HBASE_MASTER_LOADBALANCER_CLASS;
    String configured = conf.get(key, LoadBalancerFactory.getDefaultLoadBalancerClass().getName());

    // Ensure not null or empty
    assertNotNull("Configuration " + key + " should not be null", configured);
    assertFalse("Configuration " + key + " should not be empty", configured.isEmpty());

    // Validate class existence and interface implementation
    try {
      Class<?> clazz = Class.forName(configured);
      assertTrue("Configured class should implement LoadBalancer",
          LoadBalancer.class.isAssignableFrom(clazz));
    } catch (ClassNotFoundException e) {
      fail("Configured load balancer class not found: " + configured);
    }

    // Validate default
    String defaultName = LoadBalancerFactory.getDefaultLoadBalancerClass().getName();
    if (conf.get(key, null) == null) {
      // Not explicitly set, should match default
      assertEquals("Default load balancer class mismatch",
          defaultName, configured);
    }
  }
}
