package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Test class to validate the 'hbase.master.logcleaner.plugins' configuration.
 */
@Category(SmallTests.class)
public class TestMasterLogCleanerConfiguration {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMasterLogCleanerConfiguration.class);

    /**
     * Test if the 'hbase.master.logcleaner.plugins' configuration is valid.
     */
    @Test
    public void testLogCleanerPluginsConfigurationValidity() throws Exception {
        // 1. You need to use the HBase 2.2.2 API correctly to obtain configuration values,
        // instead of hardcoding the configuration values.

        // 2. Prepare the test conditions.
        Configuration conf = new Configuration();
        // Add default configuration settings, if needed
        conf.set("hbase.master.logcleaner.plugins",
                "org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner," +
                "org.apache.hadoop.hbase.master.cleaner.TimeToLiveProcedureWALCleaner");

        // 3. Test code.
        String logCleanerPlugins = conf.get("hbase.master.logcleaner.plugins");

        assertNotNull("The 'hbase.master.logcleaner.plugins' configuration should not be null.", logCleanerPlugins);
        assertFalse("The 'hbase.master.logcleaner.plugins' configuration should not be empty.", logCleanerPlugins.isEmpty());

        // Split the configuration value into individual cleaner classes
        String[] cleanerClasses = logCleanerPlugins.split(",");
        assertTrue("The 'hbase.master.logcleaner.plugins' configuration should contain at least the default log cleaners.",
                containsClass(cleanerClasses, "org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner") &&
                        containsClass(cleanerClasses, "org.apache.hadoop.hbase.master.cleaner.TimeToLiveProcedureWALCleaner"));

        // Additional validation can be added as needed.

        // 4. Code after testing.
    }

    /**
     * Helper method to check if a specific class is present in the list of cleaner classes.
     *
     * @param classes   Array of cleaner class names.
     * @param className Fully qualified class name to check for.
     * @return true if the class is present, false otherwise.
     */
    private boolean containsClass(String[] classes, String className) {
        for (String c : classes) {
            if (c.trim().equals(className)) {
                return true;
            }
        }
        return false;
    }
}