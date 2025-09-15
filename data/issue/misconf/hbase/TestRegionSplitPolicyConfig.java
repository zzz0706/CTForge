package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

//HBase-23581
@Category(SmallTests.class)
public class TestRegionSplitPolicyConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestRegionSplitPolicyConfig.class);

    private static final String CONF_KEY = "hbase.regionserver.region.split.policy";

    @Test
    public void testPolicyResolutionWithFallback() throws Exception {
        // Load actual HBase configuration (hbase-default.xml, hbase-site.xml)
        Configuration conf = HBaseConfiguration.create();
        String configured = conf.get(CONF_KEY);

        assertNotNull("Configuration key '" + CONF_KEY + "' must be set", configured);
        configured = configured.trim();

        // Build a TableDescriptor without overriding the split policy
        TableDescriptor htd = TableDescriptorBuilder.newBuilder(
                TableName.valueOf("testTable"))
                .build();

        // Resolve the split policy class based on loaded configuration
        Class<? extends RegionSplitPolicy> resolved = RegionSplitPolicy.getSplitPolicyClass(htd, conf);
        assertNotNull("Resolved policy class should not be null", resolved);

        // Derive simple class name from configured value
        String simpleName = configured.contains(".")
                ? configured.substring(configured.lastIndexOf('.') + 1)
                : configured;

        if (resolved.getSimpleName().equals(simpleName)) {
            // Valid policy: simple names match
            assertEquals(
                    "Resolved policy should match configured class",
                    simpleName,
                    resolved.getSimpleName());
        } else {
            // Invalid policy: should fall back to ConstantSizeRegionSplitPolicy
            assertEquals(
                    "Invalid policy '" + configured + "' should fall back to ConstantSizeRegionSplitPolicy",
                    ConstantSizeRegionSplitPolicy.class,
                    resolved);
        }
    }
}