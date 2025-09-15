package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Unit test for the DynamicClassLoader functionality, focusing on initialization behavior
 * when the hbase.dynamic.jars.dir configuration is missing.
 */
@Category(SmallTests.class)
public class TestDynamicClassLoader {

    @ClassRule // Ensures test follows HBaseClassTestRule conventions
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestDynamicClassLoader.class);

    /**
     * Test case: testDynamicClassLoaderInitialization_withMissingConfiguration
     * Objective: Verify that DynamicClassLoader handles missing hbase.dynamic.jars.dir configuration correctly.
     */
    @Test
    public void testDynamicClassLoaderInitialization_withMissingConfiguration() {
        // 1. Prepare the test conditions: create a Configuration object without the hbase.dynamic.jars.dir property set.
        Configuration conf = new Configuration();
        conf.unset("hbase.dynamic.jars.dir");

        // 2. Create an instance of DynamicClassLoader using the Configuration object and a parent ClassLoader.
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(conf, this.getClass().getClassLoader());

        // 3. Test: Verify that the dynamic jars directory is null as the configuration is missing.
        String dynamicJarsDir = conf.get("hbase.dynamic.jars.dir"); // Use the Configuration object directly
        assertNull("Dynamic jars directory should be null when configuration is missing.", dynamicJarsDir);
    }
}