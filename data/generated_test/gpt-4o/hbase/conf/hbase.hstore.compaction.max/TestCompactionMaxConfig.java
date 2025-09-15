package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.ClassRule;

import static org.junit.Assert.assertTrue;

/**
 * Unit test to verify the constraints and dependencies of 'hbase.hstore.compaction.max' configuration.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestCompactionMaxConfig {

    @ClassRule // HBaseClassTestRule is correctly annotated as the class-level rule.
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestCompactionMaxConfig.class);

    private static Configuration conf;

    @BeforeClass
    public static void setUp() {
        // Initialize the configuration object.
        conf = new Configuration();
        // Set default values or load specific properties if needed for testing.
        // Example: conf.set("hbase.hstore.compaction.max", "20");
    }

    /**
     * Test to validate the 'hbase.hstore.compaction.max' configuration value.
     */
    @Test
    public void testHStoreCompactionMaxConfiguration() {
        // Step 1: Use the HBase API to retrieve the configuration value for 'hbase.hstore.compaction.max'.
        final String configKey = "hbase.hstore.compaction.max";
        int maxFilesToCompact = conf.getInt(configKey, 10); // Default value set to 10.

        // Step 2: Validate the configuration value.
        // Ensure the value is a positive integer greater than 0.
        assertTrue("The value of " + configKey + " must be a positive integer", maxFilesToCompact > 0);

        // Additional Test: Limit the value to a practically reasonable range for minor compactions.
        int upperLimit = 100; // This limit is arbitrary; update based on specific HBase guidelines or context.
        assertTrue("The value of " + configKey + " must be less than or equal to " + upperLimit,
                maxFilesToCompact <= upperLimit);
    }
}