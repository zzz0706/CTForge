package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.ClassRule;

import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionMemStoreBlockMultiplier {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestHRegionMemStoreBlockMultiplier.class);

    /**
     * Validates that the configuration value hbase.hregion.memstore.block.multiplier
     * adheres to the expected constraints for HBase 2.2.2.
     */
    @Test
    public void testHRegionMemStoreBlockMultiplierConfiguration() {
        // Step 1: Load the HBase configuration
        Configuration config = new Configuration();

        // Step 2: Retrieve the configuration value using the API
        long blockMultiplier = config.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);

        // Step 3: Validate the configuration value against constraints
        // The configuration should be a positive integer greater than zero
        assertTrue("hbase.hregion.memstore.block.multiplier should be a positive integer greater than zero", blockMultiplier > 0);
    }
}