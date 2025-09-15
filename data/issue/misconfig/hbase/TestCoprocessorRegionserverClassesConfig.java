package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

//HBASE-21148
@Category(SmallTests.class)
public class TestCoprocessorRegionserverClassesConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
            .forClass(TestCoprocessorRegionserverClassesConfig.class);

    private static final String CONF_KEY = "hbase.coprocessor.regionserver.classes";

    /**
     * Fetches the class names from the configuration property,
     * splits on commas, and verifies each class can be loaded via reflection.
     */
    @Test
    public void testCoprocessorClassesLoadable() {
        Configuration conf = HBaseConfiguration.create();
        String raw = conf.get(CONF_KEY);

        assertNotNull("Configuration key '" + CONF_KEY + "' should be set", raw);

        String[] classNames = raw.split(",");
        for (String name : classNames) {
            String trimmed = name.trim();
            try {
                Class.forName(trimmed);
            } catch (ClassNotFoundException e) {
                fail("Class '" + trimmed + "' configured in " + CONF_KEY + " not found");
            }
        }
    }
}
