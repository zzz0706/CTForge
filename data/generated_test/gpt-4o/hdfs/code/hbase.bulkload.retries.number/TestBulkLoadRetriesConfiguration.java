package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({MiscTests.class, SmallTests.class})
public class TestBulkLoadRetriesConfiguration {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestBulkLoadRetriesConfiguration.class);

    /**
     * Tests bulk load retries configuration by setting a value,
     * retrieving it, and validating it against specific conditions.
     */
    @Test
    public void testBulkLoadRetriesConfiguration() {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();

        // Set a test value for the configuration key to ensure it can be retrieved correctly during the test.
        conf.setInt(HConstants.BULKLOAD_MAX_RETRIES_NUMBER, 15);

        // Fetch the configuration value using the API.
        int retriesValue = conf.getInt(HConstants.BULKLOAD_MAX_RETRIES_NUMBER, 10); // Default value is 10.

        // 2. Prepare the test conditions.
        // The fetched value should match the test value (15).
        assertEquals("The configuration value should match the set value", 15, retriesValue);

        // 3. Test code.
        // Validate that the configuration value matches the requirements (non-negative).
        assertTrue(
            "The value of hbase.bulkload.retries.number must be a non-negative integer.",
            retriesValue >= 0
        );

        // Additionally, make sure the value is propagated properly (if required by your logic).
        System.out.println("Configuration `hbase.bulkload.retries.number` passed all validation checks with value: " + retriesValue);

        // 4. Code after testing.
        // Perform cleanup if necessary.
        // NOTE: Cleanup is not required for this test.
    }
}