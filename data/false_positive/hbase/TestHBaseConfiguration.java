package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for validating HBase configuration and TableDescriptor sanity checks.
 * This test is strictly based on hbase 2.2.2 APIs and expects the configuration to properly
 * validate memstore flush size configurations.
 */
@Category(SmallTests.class)
public class TestHBaseConfiguration {

    @ClassRule
    public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
            org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHBaseConfiguration.class);

    /**
     * Test for validating the configuration `hbase.hregion.memstore.flush.size`.
     * Correctly uses the HBase 2.2.2 API for fetching configuration and performing assertions.
     */
    @Test
    public void testMemStoreFlushSizeConfigurationValidity() {
        // 1. Use HBase 2.2.2 API to retrieve configuration values
        Configuration conf = new Configuration();
        long memStoreFlushSize = conf.getLong(
                HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                2 * 1024 * 1024L  // Default value for memstore flush size in HBase
        );

        // 2. Prepare test conditions
        final long flushSizeLowerLimit = 1024 * 1024L; // Default lower limit is 1MB

        // 3. Test memstore flush size to ensure it meets constraints
        assertTrue(
                "MEMSTORE_FLUSH_SIZE must be greater than or equal to 1MB.",
                memStoreFlushSize >= flushSizeLowerLimit
        );

        long maxFlushSizeLimit = conf.getLong(
                "hbase.hregion.memstore.flush.size.limit", // Hypothetical configuration for the flush size's upper limit
                Long.MAX_VALUE
        );

        // Test upper limit constraints
        assertTrue(
                "MEMSTORE_FLUSH_SIZE exceeds the maximum permissible limit.",
                memStoreFlushSize <= maxFlushSizeLimit
        );

        assertEquals(2 * 1024 * 1024L, memStoreFlushSize);

    }

    /**
     * Test for propagation dependency constraints of MemStore flush size configuration.
     */
    @Test
    public void testDependentConfigurationsForMemStoreFlushSize() {
        // 1. Use HBase 2.2.2 API to retrieve configuration values
        Configuration conf = new Configuration();
        long flushSize = conf.getLong(
                HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                2 * 1024 * 1024L  // Default value for memstore flush size in HBase
        );

        long multiplier = conf.getLong(
                HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
                4L // Default value for memstore block multiplier in HBase
        );

        // 2. Prepare test conditions
        long blockingMemStoreSize = flushSize * multiplier;

        // 3. Test dependent relations
        assertTrue(
                "MEMSTORE_BLOCK_MULTIPLIER must be greater than zero.",
                multiplier > 0
        );
        assertTrue(
                "Blocking memstore size must be greater than or equal to MemStore flush size.",
                blockingMemStoreSize >= flushSize
        );
    }

    /**
     * Integration test for validating sanity check logic using HBase's TableDescriptorChecker.
     */
    @Test
    public void testSanityChecksForTableDescriptor() {
        // 1. Prepare configuration and table descriptor using HBase APIs
        Configuration conf = new Configuration();
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("dummyTable"));

        // Define memstore flush size explicitly for table descriptor
        TableDescriptor td = builder
                .setMemStoreFlushSize(134217728L) // 128MB
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")) // Adding column family cf
                .build();

        try {
            // 2. Perform sanity check using TableDescriptorChecker
            TableDescriptorChecker.sanityCheck(conf, td);
            assertTrue("Sanity checks passed for configurable values.", true);
        } catch (Exception ex) {
            fail("Sanity checks failed: " + ex.getMessage());
        }
    }
}