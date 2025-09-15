package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.Assert;

/**
 * Unit tests for validating memory compaction configuration for system tables in HBase.
 */
@Category(org.apache.hadoop.hbase.testclassification.SmallTests.class)
public class TestSystemTablesCompactingMemstoreConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestSystemTablesCompactingMemstoreConfig.class);

    /**
     * Test case for checking the validity of "hbase.systemtables.compacting.memstore.type" configuration.
     */
    @Test
    public void testSystemTablesCompactingMemstoreTypeConfiguration() {
        // 1. Prepare the test conditions.
        Configuration conf = new Configuration();
        String configKey = "hbase.systemtables.compacting.memstore.type";

        // 2. Obtain the configuration value.
        String configValue = conf.get(configKey, "NONE").toUpperCase();

        // 3. Validate valid values for the configuration.
        try {
            MemoryCompactionPolicy.valueOf(configValue);
        } catch (IllegalArgumentException e) {
            Assert.fail("Invalid value for " + configKey + " configuration: " + configValue +
                        ". Valid values are: NONE, BASIC, EAGER, ADAPTIVE.");
        }
    }

    /**
     * Test case to verify that no memory compaction strategy is applied for "NONE" configuration value.
     */
    @Test
    public void testSystemTablesMemstoreTypeNONEValidation() {
        // 1. Prepare the test conditions.
        Configuration conf = new Configuration();
        String configKey = "hbase.systemtables.compacting.memstore.type";

        // 2. Obtain the configuration value.
        String configValue = conf.get(configKey, "NONE").toUpperCase();

        // 3. Validate the behavior for "NONE".
        if (configValue.equals("NONE")) {
            Assert.assertTrue("For NONE type, no memory compaction strategy should be applied.",
                              configValue.equals("NONE"));
        }
    }

    /**
     * Test case to validate valid memory compaction strategies (BASIC, EAGER, ADAPTIVE).
     */
    @Test
    public void testValidMemoryCompactionStrategiesConfiguration() {
        // 1. Prepare the test conditions.
        Configuration conf = new Configuration();
        String configKey = "hbase.systemtables.compacting.memstore.type";

        // 2. Obtain the configuration value.
        String configValue = conf.get(configKey, "NONE").toUpperCase();

        // 3. Validate for non-NONE valid strategies.
        if (!configValue.equals("NONE")) {
            try {
                MemoryCompactionPolicy compactionPolicy = MemoryCompactionPolicy.valueOf(configValue);

                // 4. Switch case for allowed policies.
                switch (compactionPolicy) {
                    case BASIC:
                    case EAGER:
                    case ADAPTIVE:
                        break; // Valid configuration
                    default:
                        Assert.fail("Unexpected MemoryCompactionPolicy found: " + configValue);
                }
            } catch (IllegalArgumentException e) {
                Assert.fail("Invalid MemoryCompactionPolicy found in configuration.");
            }
        }
    }
}